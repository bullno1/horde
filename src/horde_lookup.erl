-module(horde_lookup).
-behaviour(gen_server).
% API
-export([start_link/1]).
% gen_server
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2
]).
% Internal
-export([proc_entry/2]).

-record(state, {
	horde :: pid(),
	sender :: horde_utils:reply_to(),
	address :: horde:overlay_address(),
	max_address :: horde:overlay_address(),
	num_parallel_queries :: pos_integer(),
	tracer :: horde_tracer:tracer(),
	num_pending_queries = 0 :: non_neg_integer(),
	next_resolvers = gb_trees:empty()
		:: gb_trees:tree({integer(), horde:endpoint()}, horde:endpoint()),
	queried_resolvers = sets:new() :: sets:set(horde:endpoint())
}).

% API

start_link(Opts) -> proc_lib:start_link(?MODULE, proc_entry, [self(), Opts]).

% gen_server

init(#{
	horde := Horde,
	sender := Sender,
	address := Address,
	max_address := MaxAddress,
	num_parallel_queries := NumParallelQueries,
	tracer := Tracer
} = Opts) ->
	horde_tracer:handle_event({start, Opts}, Tracer),
	State = #state{
		horde = Horde,
		sender = Sender,
		address = Address,
		max_address = MaxAddress,
		num_parallel_queries = NumParallelQueries,
		tracer = Tracer
	},
	{ok, State}.

handle_call(_, _, State) -> {stop, unimplemented, State}.

handle_cast(_, State) -> {stop, unimplemented, State}.

handle_info(
	{horde, Ref, noreply},
	#state{
		tracer = Tracer,
		num_pending_queries = NumPendingQueries
	} = State
) ->
	horde_tracer:handle_event({noreply, Ref}, Tracer),
	send_next_query(State#state{num_pending_queries = NumPendingQueries - 1});
handle_info(
	{horde, Ref, {reply, {peer_info, Peers}}},
	#state{
		address = OverlayAddress,
		sender = Sender,
		num_pending_queries = NumPendingQueries,
		tracer = Tracer
	} = State
) ->
	horde_tracer:handle_event(
		{reply, #{reference => Ref, result => Peers}},
		Tracer
	),
	State2 = State#state{num_pending_queries = NumPendingQueries - 1},
	case lookup_finished(OverlayAddress, Peers) of
		{true, TransportAddress} ->
			Result = {ok, TransportAddress},
			horde_tracer:handle_event({finish, Result}, Tracer),
			horde_utils:maybe_reply(horde, Sender, Result),
			{stop, normal, State2};
		false ->
			Endpoints = [{compound, Address} || #{address := Address} <- Peers],
			send_next_query(add_resolvers(Endpoints, State2))
	end;
handle_info(Msg, State) ->
	error_logger:warning_report([
		"Unexpected message", {module, ?MODULE}, {message, Msg}
	]),
	{noreply, State}.

% Internal

proc_entry(Parent, #{resolvers := Resolvers} = Opts) ->
	{ok, State} = init(Opts),
	proc_lib:init_ack(Parent, {ok, self()}),
	case send_next_query(add_resolvers(Resolvers, State)) of
		{noreply, State2} ->
			gen_server:enter_loop(?MODULE, [], State2);
		{stop, Reason, _} ->
			exit(Reason)
	end.

% Private

distance({transport, _}, _, MaxAddress) -> MaxAddress;
distance({compound, #{overlay := From}}, To, MaxAddress) ->
	min(abs(To - From), MaxAddress + 1 - max(From, To) + min(From, To)).

add_resolvers(
	Resolvers,
	#state{
		address = LookupAddress,
		max_address = MaxAddress,
		queried_resolvers = QueriedResolvers,
		next_resolvers = NextResolvers
	} = State
) ->
	NextResolvers2 = lists:foldl(
		fun(Resolver, Acc) ->
			case sets:is_element(Resolver, QueriedResolvers) of
				true ->
					Acc;
				false ->
					Distance = distance(Resolver, LookupAddress, MaxAddress),
					gb_trees:enter({Distance, Resolver}, Resolver, Acc)
			end
		end,
		NextResolvers,
		Resolvers
	),
	State#state{next_resolvers = NextResolvers2}.

lookup_finished(LookupAddress, Peers) ->
	case lists:dropwhile(
		fun(#{address := #{overlay := OverlayAddress}}) ->
			OverlayAddress =/= LookupAddress
		end,
		Peers
	) of
		[#{address := #{transport := TransportAddress}} | _] ->
			{true, TransportAddress};
		[] ->
			false
	end.

send_next_query(
	#state{
		address = Address,
		sender = Sender,
		num_parallel_queries = NumParallelQueries,
		num_pending_queries = NumPendingQueries,
		next_resolvers = NextResolvers,
		queried_resolvers = QueriedResolvers,
		tracer = Tracer,
		horde = Horde
	} = State
) ->
	NumNextResolvers = gb_trees:size(NextResolvers),
	if
		NumNextResolvers =:= 0, NumPendingQueries =:= 0 ->
			% Nothing more we could do
			horde_tracer:handle_event({finish, error}, Tracer),
			horde_utils:maybe_reply(horde, Sender, error),
			{stop, normal, State};
		NumNextResolvers =:= 0, NumPendingQueries > 0 ->
			% Wait for pending queries to finish
			{noreply, State};
		NumPendingQueries >= NumParallelQueries ->
			% Reached parallel queries limit, wait until some finishes
			{noreply, State};
		true ->
			% Send next query
			{{Distance, _}, NextResolver, NextResolvers2} =
				gb_trees:take_smallest(NextResolvers),
			Ref = horde:send_query_async(Horde, NextResolver, {lookup, Address}),
			QueryInfo = #{
				reference => Ref,
				endpoint => NextResolver,
				distance => Distance
			},
			horde_tracer:handle_event({query, QueryInfo}, Tracer),
			State2 = State#state{
				next_resolvers = NextResolvers2,
				num_pending_queries = NumPendingQueries + 1,
				queried_resolvers = sets:add_element(NextResolver, QueriedResolvers)
			},
			% Send more if we have not reached the limit
			send_next_query(State2)
	end.
