-module(horde).
-behaviour(gen_server).
% API
-export([
	start_link/1,
	start_link/2,
	lookup/3,
	send_query/3
]).
% gen_server
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2
]).
-export_type([
	name/0,
	ref/0,
	overlay_address/0,
	compound_address/0,
	endpoint/0,
	node_info/0,
	message_header/0,
	message_body/0
]).

-type overlay_address() :: non_neg_integer().
-type compound_address() :: #{
	overlay := overlay_address(),
	transport := horde_transport:address()
}.
-type endpoint()
	:: {compound, compound_address()}
	 | {transport, horde_transport:address()}.
-type message_header() :: #{
	from := compound_address(),
	id => term(),
	timestamp := non_neg_integer()
}.
-type message_body() ::
	  {lookup, overlay_address()}
	| ping
	| {peer_info, [node_info()]}
	| {user, binary()}.
-type node_info() :: #{
	address := compound_address(),
	last_seen := non_neg_integer(),
	source => direct | indirect
}.
-type name() :: {local, atom()} | {via, module(), term()}.
-type ref() :: pid() | atom() | {via, module(), term()}.
-type opts() :: #{
	keypair := horde_crypto:keypair(),
	transport := {Module :: module(), Opts :: term()},
	bootstrap_nodes => [horde_transport:address()],
	ring_check_interval => non_neg_integer(),
	query_timeout => pos_integer(),
	num_retries => non_neg_integer(),
	retry_delay => non_neg_integer(),
	num_parallel_queries => pos_integer(),
	num_next_hops => pos_integer(),
	num_neighbours => pos_integer(),
	min_slice_size => pos_integer()
}.

-record(query, {
	sender :: pid() | none,
	destination :: endpoint(),
	message :: message_body(),
	num_retries :: non_neg_integer(),
	timer :: reference() | undefined
}).
-record(state, {
	crypto :: horde_crypto:ctx(),
	address :: overlay_address(),
	max_address :: overlay_address(),
	keypair :: horde_crypto:keypair(),
	transport :: horde_transport:ref(),
	queries = #{} :: #{reference() => #query{}},
	successor :: node_info() | undefined,
	predecessor :: node_info() | undefined,
	ring :: horde_ring:ring(overlay_address(), node_info()),
	ring_check_timer :: reference(),
	ring_check_interval :: non_neg_integer(),
	num_nodes_probed = 0 :: non_neg_integer(),
	num_nodes_failed = 0 :: non_neg_integer(),
	query_timeout :: pos_integer(),
	retry_delay :: pos_integer(),
	num_retries :: pos_integer(),
	num_parallel_queries :: pos_integer(),
	num_next_hops :: pos_integer(),
	num_neighbours :: pos_integer(),
	min_slice_size :: pos_integer()
}).

% API

-spec start_link(opts()) -> {ok, pid()} | {error, any()}.
start_link(Opts) -> gen_server:start_link(?MODULE, Opts, []).

-spec start_link(name(), opts()) -> {ok, pid()} | {error, any()}.
start_link(Name, Opts) -> gen_server:start_link(Name, ?MODULE, Opts, []).

-spec lookup(ref(), overlay_address(), timeout())
	-> {ok, horde_transport:address()}
	 | {error, term()}.
lookup(Ref, Address, Timeout) ->
	gen_server:call(Ref, {lookup, Address}, Timeout).

-spec send_query(ref(), endpoint(), message_body()) -> reference().
send_query(Ref, Endpoint, Message) ->
	QueryRef = make_ref(),
	gen_server:cast(Ref, {send_query, QueryRef, self(), Endpoint, Message}),
	QueryRef.

% gen_server

init(#{
	crypto := {CryptoMod, CryptoOpts},
	keypair := {PubKey, _} = KeyPair,
	transport := {TransportMod, TransportOpts}
} = Opts) ->
	case horde_transport:open(TransportMod, TransportOpts) of
		{ok, Transport} ->
			horde_transport:recv_async(Transport),
			Crypto = horde_crypto:init(CryptoMod, CryptoOpts),
			OverlayAddress = horde_crypto:address_of(Crypto, PubKey),
			RingCheckInterval = maps:get(ring_check_interval, Opts, 60000),
			RingCheckTimer = start_timer(RingCheckInterval, check_ring),
			MaxAddress = horde_crypto:info(Crypto, max_address),
			RevFun = fun(Addr) -> MaxAddress - Addr end,
			State = #state{
				crypto = Crypto,
				address = OverlayAddress,
				max_address = MaxAddress,
				keypair = KeyPair,
				transport = Transport,
				ring = horde_ring:new(RevFun),
				ring_check_timer = RingCheckTimer,
				ring_check_interval = RingCheckInterval,
				query_timeout = maps:get(query_timeout, Opts, 5000),
				num_retries = maps:get(num_retries, Opts, 3),
				retry_delay = maps:get(retry_delay, Opts, 500),
				num_parallel_queries = maps:get(num_parallel_queries, Opts, 3),
				num_next_hops = maps:get(num_next_hops, Opts, 3),
				num_neighbours = maps:get(num_neighbours, Opts, 5),
				min_slice_size = maps:get(min_slice_size, Opts, 2)
			},
			% Bootstrap
			BootstrapNodes = maps:get(bootstrap_nodes, Opts, []),
			case length(BootstrapNodes) > 0 of
				true ->
					LookupEndpoints = [
						{transport, Address} || Address <- BootstrapNodes
					],
					start_lookup(OverlayAddress, LookupEndpoints, none, State);
				false ->
					ok
			end,
			{ok, State};
		{error, Reason} ->
			{stop, Reason}
	end.

handle_call(
	{lookup, Address}, From,
	#state{
		address = OwnAddress,
		ring = Ring,
		num_parallel_queries = NumParallelQueries,
		num_neighbours = NumNeighbours
	} = State
) ->
	Peers = ordsets:union([
		horde_ring:successors(Address, 1, Ring),
		horde_ring:predecessors(Address, NumParallelQueries - 1, Ring),
		horde_ring:successors(OwnAddress, NumNeighbours, Ring),
		horde_ring:predecessors(OwnAddress, NumNeighbours, Ring)
	]),
	LookupEndpoints = [{compound, Addr} || #{address := Addr} <- Peers],
	start_lookup(Address, LookupEndpoints, From, State),
	{noreply, State}.

handle_cast(
	{send_query, Ref, Sender, Endpoint, Message},
	#state{
		num_retries = NumRetries,
		num_nodes_probed = NumNodesProbed
	} = State
) ->
	Query = #query{
		sender = Sender,
		destination = Endpoint,
		message = Message,
		num_retries = NumRetries
	},
	State2 = send_query1(Ref, Query, State),
	State3 = State2#state{num_nodes_probed = NumNodesProbed + 1},
	{noreply, State3}.

handle_info({timeout, _, {query_timeout, Id}} = Event, State) ->
	State2 = dispatch_query_event(
		Event, fun handle_query_error/4, Id, State
	),
	{noreply, State2};
handle_info(
	{horde_transport, Transport, {message, Header, Body}},
	#state{transport = Transport} = State
) ->
	State2 = handle_overlay_message(Header, Body, State),
	horde_transport:recv_async(Transport),
	{noreply, State2};
handle_info(
	{horde_transport, Transport, {transport_error, _, Id} = Error},
	#state{transport = Transport} = State
) ->
	State2 = dispatch_query_event(Error, fun handle_query_error/4, Id, State),
	{noreply, State2};
handle_info(Msg, State) ->
	error_logger:warning_report([
		{?MODULE, "Unexpected message"},
		{message, Msg}
	]),
	{noreply, State}.

terminate(_, #state{transport = Transport}) ->
	_ = horde_transport:close(Transport),
	ok.

% Private

start_lookup(
	Address, LookupEndpoints, ReplyTo,
	#state{
		num_parallel_queries = NumParallelQueries,
		max_address = MaxAddress
	}
) ->
	LookupArgs = #{
		address => Address,
		sender => ReplyTo,
		endpoints => LookupEndpoints,
		max_address => MaxAddress,
		num_parallel_queries => NumParallelQueries
	},
	{ok, _} = horde_lookup:start_link(LookupArgs),
	ok.

handle_overlay_message(
	#{from := Address, timestamp := Timestamp} = Header, Body,
	State
) ->
	State2 = handle_overlay_message1(Header, Body, State),
	Node = #{address => Address, last_seen => Timestamp, source => direct},
	add_node(Node, State2).

handle_overlay_message1(
	#{from := Sender} = Header, {lookup, TargetAddress},
	#state{
		ring = Ring,
		address = OwnAddress,
		successor = OwnSuccessor,
		predecessor = OwnPredecessor,
		num_next_hops = NumNextHops
	} = State
) ->
	Nodes = case horde_ring:lookup(TargetAddress, Ring) of
		% If node is known, reply with info and own immediate predecessor
		{value, Node} ->
			ordsets:add_element(Node, set_of(OwnPredecessor));
		% If node is not known, reply with a closer immediate neighbour and
		% a number of "next best hops" around the target.
		none ->
			{CloserNeighbour, NumSuccessors, NumPredecessors} =
				case horde_address:is_between(
					OwnAddress,
					overlay_address(Sender),
					TargetAddress
				) of
					true -> {OwnSuccessor, 1, NumNextHops - 1};
					false -> {OwnPredecessor, NumNextHops - 1, 1}
				end,
			ordsets:union([
				set_of(CloserNeighbour),
				horde_ring:successors(TargetAddress, NumSuccessors, Ring),
				horde_ring:predecessors(TargetAddress, NumPredecessors, Ring)
			])
	end,
	reply(Header, {peer_info, Nodes}, State);
handle_overlay_message1(
	Header, ping,
	#state{
		ring = Ring, address = OwnAddress,
		num_neighbours = NumNeighbours
	} = State
) ->
	Nodes = ordsets:union(
		horde_ring:successors(OwnAddress, NumNeighbours, Ring),
		horde_ring:predecessors(OwnAddress, NumNeighbours, Ring)
	),
	reply(Header, {peer_info, Nodes}, State);
handle_overlay_message1(
	#{id := Id} = Header, {peer_info, Peers} = Message, State
) ->
	State2 = dispatch_query_event(
		{Header, Message},
		fun handle_query_reply/4,
		Id, State
	),
	lists:foldl(
		fun(Peer, Acc) -> add_node(Peer#{source => indirect}, Acc) end,
		State2,
		Peers
	).

reply(
	#{from := #{transport := Sender}, id := Id},
	Message,
	#state{transport = Transport} = State
) ->
	horde_transport:send(Transport, Sender, Id, Message),
	State.

dispatch_query_event(Event, Handler, Id, #state{queries = Queries} = State) ->
	case maps:find(Id, Queries) of
		{ok, Query} -> Handler(Event, Id, Query, State);
		error -> State
	end.

handle_query_reply(
	{#{from := #{transport := TransportAddress} = CompoundAddress}, Message},
	Id, #query{sender = Sender, destination = Destination, timer = Timer},
	#state{queries = Queries} = State
) ->
	IsCorrectReply =
		case Destination of
			{compound, CompoundAddress} -> true;
			{transport, TransportAddress} -> true;
			_ -> false
		end,
	case IsCorrectReply of
		true ->
			_ = maybe_send(Sender, {?MODULE, Id, {reply, Message}}),
			cancel_timer(Timer),
			State#state{queries = maps:remove(Id, Queries)};
		false ->
			State
	end.

handle_query_error(
	{timeout, TimerRef, {query_timeout, Id}},
	Id, #query{timer = TimerRef} = Query,
	State
) ->
	handle_query_timeout(Id, Query, State);
handle_query_error(
	{transport_error, _, Id},
	Id, Query,
	State
) ->
	handle_transport_error(Id, Query, State);
handle_query_error(_, _, _, State) ->
	State.

handle_query_timeout(
	Id, #query{num_retries = 0, sender = Sender, destination = Destination},
	#state{queries = Queries, num_nodes_failed = NumNodesFailed} = State
) ->
	_ = maybe_send(Sender, {?MODULE, Id, noreply}),
	State2 = State#state{
		queries = maps:remove(Id, Queries),
		num_nodes_failed = NumNodesFailed + 1
	},
	maybe_remove_node(Destination, State2);
handle_query_timeout(
	Id, #query{num_retries = NumRetries} = Query,
	State
) ->
	Query2 = Query#query{num_retries = NumRetries - 1},
	send_query1(Id, Query2, State).

handle_transport_error(
	Id, #query{timer = TimerRef} = Query,
	#state{queries = Queries, retry_delay = RetryDelay} = State
) ->
	cancel_timer(TimerRef),
	Query2 = Query#query{
		timer = start_timer(RetryDelay, {query_timeout, Id})
	},
	State#state{
		queries = Queries#{Id := Query2}
	}.

send_query1(
	Id,
	#query{destination = Destination, message = Message} = Query,
	#state{
		queries = Queries,
		query_timeout = QueryTimeout,
		transport = Transport
	} = State
) ->
	TransportAddress =
		case Destination of
			{compound, #{transport := Address}} -> Address;
			{transport, Address} -> Address
		end,
	horde_transport:send(Transport, TransportAddress, Id, Message),
	Query2 = Query#query{
		timer = start_timer(QueryTimeout, {query_timeout, Id})
	},
	State#state{
		queries = Queries#{Id => Query2}
	}.

add_node(Node, #state{ring = Ring} = State) ->
	State2 = State#state{ring = maybe_update_ring(Node, Ring)},
	State3 = maybe_update_neighbour(predecessor, Node, State2),
	maybe_update_neighbour(successor, Node, State3).

maybe_update_ring(#{last_seen := Timestamp} = Node, Ring) ->
	OverlayAddress = overlay_address(Node),
	case horde_ring:lookup(OverlayAddress, Ring) of
		{value, #{last_seen := CacheTimestamp}} when CacheTimestamp < Timestamp ->
			horde_ring:update(OverlayAddress, Node, Ring);
		_ ->
			horde_ring:insert(OverlayAddress, Node, Ring)
	end.

maybe_update_neighbour(_, [], State) ->
	State;
maybe_update_neighbour(Position, [Node], State) ->
	maybe_update_neighbour(Position, Node, State);
maybe_update_neighbour(
	predecessor, Node, #state{predecessor = undefined} = State
) when is_map(Node) ->
	maybe_set_neighbour(predecessor, Node, State);
maybe_update_neighbour(
	successor, Node, #state{successor = undefined} = State
) when is_map(Node) ->
	maybe_set_neighbour(successor, Node, State);
maybe_update_neighbour(
	predecessor, Node,
	#state{address = OwnAddress, predecessor = Predecessor} = State
) ->
	case horde_address:is_between(
		overlay_address(Node),
		overlay_address(Predecessor),
		OwnAddress
	) of
		true -> maybe_set_neighbour(predecessor, Node, State);
		false -> State
	end;
maybe_update_neighbour(
	successor, Node,
	#state{address = OwnAddress, successor = Successor} = State
) ->
	case horde_address:is_between(
		overlay_address(Node),
		OwnAddress,
		overlay_address(Successor)
	) of
		true -> maybe_set_neighbour(successor, Node, State);
		false -> State
	end.

maybe_set_neighbour(
	_Postion,
	#{address := Address, source := indirect},
	#state{queries = Queries, num_retries = NumRetries} = State
) ->
	case is_querying(Address, Queries) of
		true ->
			State;
		false ->
			Query = #query{
				sender = none,
				destination = {compound, Address},
				message = ping,
				num_retries = NumRetries
			},
			send_query1(make_ref(), Query, State)
	end;
maybe_set_neighbour(successor, #{source := direct} = Node, State) ->
	State#state{successor = Node};
maybe_set_neighbour(predecessor, #{source := direct} = Node, State) ->
	State#state{predecessor = Node}.

is_querying(Address, Queries) ->
	lists:any(
		fun(#query{destination = Destination}) ->
			Destination =:= {compound, Address}
		end,
		maps:values(Queries)
	).

maybe_remove_node({transport, _}, State) ->
	State;
maybe_remove_node(
	{compound, RemovedAddress},
	#state{
		address = OwnAddress,
		ring = Ring,
		successor = Successor,
		predecessor = Predecessor
	} = State
) ->
	Ring2 = horde_ring:remove(RemovedAddress, Ring),
	State2 = State#state{
		ring = Ring2,
		predecessor = undefined_if_removed(RemovedAddress, Predecessor),
		successor = undefined_if_removed(RemovedAddress, Successor)
	},
	State3 = maybe_update_neighbour(
		predecessor, horde_ring:predecessors(OwnAddress, 1, Ring2), State2
	),
	maybe_update_neighbour(
		predecessor, horde_ring:successors(OwnAddress, 1, Ring2), State3
	).

-ifdef(TEST).
start_timer(Timeout, Message) ->
	horde_mock:start_timer(Timeout, self(), Message).

cancel_timer(TimerRef) ->
	_ = horde_mock:cancel_timer(TimerRef, [{async, true}]), ok.
-else.
start_timer(Timeout, Message) ->
	erlang:start_timer(Timeout, self(), Message).

cancel_timer(TimerRef) ->
	_ = erlang:cancel_timer(TimerRef, [{async, true}]),
	ok.
-endif.

undefined_if_removed(Address, #{address := Address}) -> undefined;
undefined_if_removed(_, Node) -> Node.

overlay_address(#{address := #{overlay := Address}}) -> Address;
overlay_address(#{overlay := Address}) -> Address.

set_of(undefined) -> [];
set_of(Element) -> [Element].

maybe_send(none, _) -> ok;
maybe_send(Client, Result) -> Client ! Result.
