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
-ifdef(TEST).
-define(REPORT_HISTORY(State), report_history(State)).
-else.
-define(REPORT_HISTORY(State), ok).
-endif.

-type history()
	:: {query, #{
		reference := reference(),
		endpoint := [horde:endpoint()],
		distance := non_neg_integer()
		}}
	 | {reply, #{
		reference := reference(),
		result := [horde:node_info()]
		}}
	 | {noreply, reference()}.

-record(state, {
	horde :: pid(),
	sender :: horde_utils:reply_to(),
	address :: horde:overlay_address(),
	max_address :: horde:overlay_address(),
	num_parallel_queries :: pos_integer(),
	num_pending_queries = 0 :: non_neg_integer(),
	next_resolvers = gb_trees:empty()
		:: gb_trees:tree({non_neg_integer(), horde:endpoint()}, horde:endpoint()),
	queried_resolvers = sets:new() :: sets:set(horde:endpoint()),
	history = [] :: [history()]
}).

% API

start_link(Opts) -> proc_lib:start_link(?MODULE, proc_entry, [self(), Opts]).

% gen_server

init(#{
	horde := Horde,
	sender := Sender,
	address := Address,
	max_address := MaxAddress,
	num_parallel_queries := NumParallelQueries
}) ->
	State = #state{
		horde = Horde,
		sender = Sender,
		address = Address,
		max_address = MaxAddress,
		num_parallel_queries = NumParallelQueries
	},
	{ok, State}.

handle_call(_, _, State) -> {stop, unimplemented, State}.

handle_cast(_, State) -> {stop, unimplemented, State}.

handle_info(
	{horde, Ref, noreply},
	#state{
		history = History,
		num_pending_queries = NumPendingQueries
	} = State
) ->
	send_next_query(State#state{
		num_pending_queries = NumPendingQueries - 1,
		history = [{noreply, Ref} | History]
	});
handle_info(
	{horde, Ref, {reply, {peer_info, Peers}}},
	#state{
		address = OverlayAddress,
		sender = Sender,
		num_pending_queries = NumPendingQueries,
		history = History
	} = State
) ->
	State2 = State#state{
		num_pending_queries = NumPendingQueries - 1,
		history = [{reply, #{reference => Ref, result => Peers}} | History]
	},
	case lookup_finished(OverlayAddress, Peers) of
		{true, TransportAddress} ->
			%case
				%length(State2#state.history) > State2#state.num_parallel_queries + 1
			%of
				%true -> ?REPORT_HISTORY(State2);
				%false -> ok
			%end,
			horde_utils:maybe_reply(horde, Sender, {ok, TransportAddress}),
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
		history = History,
		horde = Horde
	} = State
) ->
	NumNextResolvers = gb_trees:size(NextResolvers),
	if
		NumNextResolvers =:= 0, NumPendingQueries =:= 0 ->
			% Nothing more we could do
			%case length(History) > 2 of
				%true ->
					%?REPORT_HISTORY(State);
				%false ->
					%ok
			%end,
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
			State2 = State#state{
				next_resolvers = NextResolvers2,
				history = [
					{query, #{
						reference => Ref,
						endpoint => NextResolver,
						distance => Distance
					}} | History
				],
				num_pending_queries = NumPendingQueries + 1,
				queried_resolvers = sets:add_element(NextResolver, QueriedResolvers)
			},
			% Send more if we have not reached the limit
			send_next_query(State2)
	end.

-ifdef(TEST).
report_history(#state{address = Address, history = History}) ->
	ForwardHistory = lists:reverse(History),
	ct:pal("Target: ~p~nDistances: ~w~nHistory: ~n~s", [
		Address,
		[Distance || {query, #{distance := Distance}} <- ForwardHistory],
		format_history(ForwardHistory)
	]).

format_history(History) -> format_history(History, #{}, []).

format_history([], Mappings, Acc) ->
	lists:reverse(Acc);
format_history([Event | Rest], Mappings, Acc) ->
	{NewLine, NewMappings} = case Event of
		{query, #{reference := Ref, endpoint := Endpoint, distance := Distance}} ->
			QueryNo = maps:size(Mappings) + 1,
			Line = io_lib:format(
				"send #~p (distance: ~p) -> ~p~n",
				[QueryNo, Distance, Endpoint]
			),
			{Line, Mappings#{Ref => QueryNo}};
		{reply, #{reference := Ref, result := Peers}} ->
			QueryNo = maps:get(Ref, Mappings),
			Line = io_lib:format(
				"recv #~p: ~p~n",
				[QueryNo, [Address || #{address := Address} <- Peers]]
			),
			{Line, Mappings};
		{noreply, Ref} ->
			QueryNo = maps:get(Ref, Mappings),
			Line = io_lib:format(
				"fail #~p",
				[QueryNo]
			),
			{Line, Mappings}
	end,
	format_history(Rest, NewMappings, [NewLine | Acc]).
-endif.
