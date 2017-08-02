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
	next_peers = gb_trees:empty(),
	queried_peers = sets:new() :: sets:set(horde:endpoint()),
	sender :: term(),
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
		next_peers = gb_trees:from_orddict([
			{{Distance, Peer}, Peer} || {Distance, Peer} <- FallbackPeers
		]),
		queried_peers = sets:from_list([
			Peer || {_, Peer} <- InitialPeers
		])
	},
	_ = [
		horde:send_query(Horde, Peer, {lookup, Address})
		|| {_, Peer} <- InitialPeers
	],
	{ok, State}.

handle_call(_, _, State) -> {stop, unimplemented, State}.

handle_cast(_, State) -> {stop, unimplemented, State}.

handle_info({horde, _, noreply}, State) ->
	send_next_query(State);
handle_info(
	{horde, _, {reply, {peer_info, Peers}}},
	#state{address = OverlayAddress, sender = Sender} = State
) ->
	case lookup_finished(OverlayAddress, Peers) of
		{true, TransportAddress} ->
			maybe_reply(Sender, {ok, TransportAddress}),
			{stop, normal, State};
		false ->
			send_next_query(add_next_peers(Peers, State))
	end;
handle_info(Msg, State) ->
	error_logger:warning_report([
		{?MODULE, "Unexpected message"},
		{message, Msg}
	]),
	{noreply, State}.

% Private

distance({transport, _}, _, _) -> 0;
distance({compound, #{overlay := From}}, To, MaxAddress) ->
	min(
		abs(To - From),
		MaxAddress + 1 - max(From, To) + min(From, To)
	).

add_next_peers(
	Peers,
	#state{
		address = LookupAddress,
		max_address = MaxAddress,
		queried_peers = QueriedPeers,
		next_peers = NextPeers
	} = State
) ->
	lists:foldl(
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
	State#state{next_peers = NextPeers}.

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
		next_peers = NextPeers,
		queried_peers = QueriedPeers,
		horde = Horde
	} = State
) ->
	case gb_trees:size(NextPeers) =:= 0 of
		true ->
			maybe_reply(Sender, error),
			{stop, normal, State};
		false ->
			{_, NextPeer, NextPeers2} = gb_trees:take_smallest(NextPeers),
			horde:send_query(Horde, NextPeer, {lookup, Address}),
			State2 = State#state{
				next_peers = NextPeers2,
				queried_peers = sets:add_element(NextPeer, QueriedPeers)
			},
			{noreply, State2}
	end.

maybe_reply(none, _Msg) -> ok;
maybe_reply(From, Msg) -> gen_server:reply(From, Msg).
