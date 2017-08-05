-module(horde_lookup).
-behaviour(gen_server).
-export([start_link/1]).
% gen_server
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2
]).

-record(state, {
	address :: horde:overlay_address(),
	max_address :: horde:overlay_address(),
	num_parallel_queries :: pos_integer(),
	num_pending_queries :: non_neg_integer(),
	next_peers = gb_trees:empty()
		:: gb_trees:tree({non_neg_integer(), horde:endpoint()}, horde:endpoint()),
	queried_peers = sets:new() :: sets:set(horde:endpoint()),
	sender :: term(),
	history = [],
	horde :: pid()
}).

start_link(Opts) -> gen_server:start_link(?MODULE, Opts, []).

init(#{
	horde := Horde,
	sender := Sender,
	address := Address,
	peers := Peers,
	max_address := MaxAddress,
	num_parallel_queries := NumParallelQueries
}) ->
	SortedPeers = lists:sort([
		{distance(Peer, Address, MaxAddress), Peer} || Peer <- Peers
	]),
	{InitialPeers, FallbackPeers} =
		case NumParallelQueries > length(SortedPeers) of
			true -> {SortedPeers, []};
			false -> lists:split(NumParallelQueries, SortedPeers)
		end,
	State = #state{
		horde = Horde,
		sender = Sender,
		address = Address,
		max_address = MaxAddress,
		num_parallel_queries = NumParallelQueries,
		num_pending_queries = length(InitialPeers),
		next_peers = gb_trees:from_orddict([
			{{Distance, Peer}, Peer} || {Distance, Peer} <- FallbackPeers
		]),
		queried_peers = sets:from_list([
			Peer || {_, Peer} <- InitialPeers
		])
	},
	Hist = [
		{query, horde:send_query(Horde, Peer, {lookup, Address}), Peer}
		|| {_, Peer} <- InitialPeers
	],
	{ok, State#state{history = lists:reverse(Hist)}}.

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
		history = [{reply, Ref, Peers} | History]
	},
	case lookup_finished(OverlayAddress, Peers) of
		{true, TransportAddress} ->
			%case
				%length(State2#state.history) > State2#state.num_parallel_queries + 1
			%of
				%true -> report_history(State2);
				%false -> ok
			%end,
			horde_utils:maybe_reply(Sender, {ok, TransportAddress}),
			{stop, normal, State2};
		false ->
			send_next_query(add_next_peers(Peers, State2))
	end;
handle_info(Msg, State) ->
	error_logger:warning_report([
		{?MODULE, "Unexpected message"},
		{message, Msg}
	]),
	{noreply, State}.

% Private

distance({transport, _}, _, MaxAddress) -> MaxAddress;
distance({compound, #{overlay := From}}, To, MaxAddress) ->
	min(abs(To - From), MaxAddress + 1 - max(From, To) + min(From, To)).

add_next_peers(
	Peers,
	#state{
		address = LookupAddress,
		max_address = MaxAddress,
		queried_peers = QueriedPeers,
		next_peers = NextPeers
	} = State
) ->
	NextPeers2 = lists:foldl(
		fun(#{address := Address}, Acc) ->
			Peer = {compound, Address},
			case sets:is_element(Peer, QueriedPeers) of
				true ->
					Acc;
				false ->
					Distance = distance(Peer, LookupAddress, MaxAddress),
					gb_trees:enter({Distance, Peer}, Peer, Acc)
			end
		end,
		NextPeers,
		Peers
	),
	State#state{next_peers = NextPeers2}.

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
		next_peers = NextPeers,
		queried_peers = QueriedPeers,
		history = History,
		horde = Horde
	} = State
) ->
	NumNextPeers = gb_trees:size(NextPeers),
	if
		NumNextPeers =:= 0, NumPendingQueries =:= 0 ->
			% Nothing more we could do
			%report_history(State),
			horde_utils:maybe_reply(Sender, error),
			{stop, normal, State};
		NumNextPeers =:= 0, NumPendingQueries > 0 ->
			% Wait for pending queries to finish
			{noreply, State};
		NumPendingQueries >= NumParallelQueries ->
			% Reached parallel queries limit, wait until some finishes
			{noreply, State};
		true ->
			% Send next query
			{_, NextPeer, NextPeers2} = gb_trees:take_smallest(NextPeers),
			Ref = horde:send_query(Horde, NextPeer, {lookup, Address}),
			State2 = State#state{
				next_peers = NextPeers2,
				history = [{query, Ref, NextPeer} | History],
				num_pending_queries = NumPendingQueries + 1,
				queried_peers = sets:add_element(NextPeer, QueriedPeers)
			},
			% Send more if we have not reached the limit
			send_next_query(State2)
	end.

-ifdef(TEST).
report_history(#state{
	address = Address,
	max_address = MaxAddress,
	history = History
}) ->
	ForwardHistory = lists:reverse(History),
	Distances = [
		distance(Peer, Address, MaxAddress)
		|| {query, _, Peer} <- ForwardHistory
	],
	ct:pal("Target: ~p~nHistory: ~p~nDistances: ~p", [
		Address, ForwardHistory, Distances
	]).
-endif.
