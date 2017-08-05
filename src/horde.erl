-module(horde).
-behaviour(gen_server).
% API
-export([
	start_link/1,
	start_link/2,
	lookup/2,
	lookup/3,
	info/2,
	join/3,
	join_async/2,
	wait_join/2,
	ping/2,
	stop/1
]).
% Low-level API
-export([
	send_query_async/3,
	send_query_sync/3,
	query_info/2
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
-ifdef(TEST).
-define(TIME, fake_time).
-else.
-define(TIME, erlang).
-endif.

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
	address := overlay_address(),
	max_address := overlay_address(),
	transport := {module(), term()},
	ring_check_interval => non_neg_integer(),
	query_timeout => pos_integer(),
	num_retries => non_neg_integer(),
	retry_delay => non_neg_integer(),
	num_parallel_queries => pos_integer(),
	num_next_hops => pos_integer(),
	num_neighbours => pos_integer(),
	min_slice_size => pos_integer()
}.
-type query_info() :: #{
	sender := horde_utils:reply_to(),
	destination := endpoint(),
	message := message_body(),
	num_retries := non_neg_integer(),
	timer := reference() | undefined
}.
-type join_status() :: standalone | joining | ready.

-record(query, {
	sender :: horde_utils:reply_to(),
	destination :: endpoint(),
	message :: message_body(),
	num_retries :: non_neg_integer(),
	timer :: reference() | undefined
}).
-record(state, {
	address :: overlay_address(),
	max_address :: overlay_address(),
	transport :: horde_transport:ref(),
	status = standalone :: standalone | {joining, reference()} | joining2 | ready,
	join_waiters = [] :: list(),
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

-spec lookup(ref(), overlay_address())
	-> {ok, horde_transport:address()} | error.
lookup(Ref, Address) -> lookup(Ref, Address, horde_tracer:noop()).

-spec lookup(ref(), overlay_address(), horde_tracer:tracer())
	-> {ok, horde_transport:address()} | error.
lookup(Ref, Address, Tracer) ->
	gen_server:call(Ref, {lookup, Address, Tracer}, infinity).

-spec info
	(ref(), status) -> join_status();
	(ref(), ring) -> horde_ring:ring(overlay_address(), node_info());
	(ref(), queries) -> #{reference() => query_info()};
	(ref(), successor) -> node_info() | undefined;
	(ref(), predecessor) -> node_info() | undefined;
	(ref(), address) -> compound_address();
	(ref(), transport) -> horde_transport:ref().
info(Ref, Info) -> gen_server:call(Ref, {info, Info}).

-spec query_info(ref(), reference()) -> query_info() | undefined.
query_info(Ref, QueryRef) -> gen_server:call(Ref,  {query_info, QueryRef}).

-spec join(ref(), [endpoint()], timeout()) -> boolean().
join(Ref, BootstrapNodes, Timeout) ->
	join_async(Ref, BootstrapNodes),
	wait_join(Ref, Timeout).

-spec join_async(ref(), [endpoint()]) -> ok.
join_async(Ref, BootstrapNodes) when length(BootstrapNodes) > 0 ->
	gen_server:cast(Ref, {join, BootstrapNodes}).

-spec wait_join(ref(), timeout()) -> boolean().
wait_join(Ref, Timeout) ->
	gen_server:call(Ref, wait_join, Timeout).

-spec ping(ref(), endpoint()) -> pong | pang.
ping(Ref, Endpoint) ->
	case send_query_sync(Ref, Endpoint, ping) of
		{reply, _} -> pong;
		noreply -> pang
	end.

-spec send_query_async(ref(), endpoint(), message_body()) -> reference().
send_query_async(Ref, Endpoint, Message) ->
	QueryRef = make_ref(),
	gen_server:cast(Ref, {send_query, QueryRef, self(), Endpoint, Message}),
	QueryRef.

-spec send_query_sync(ref(), endpoint(), message_body()) ->
	{reply, message_body()} | noreply.
send_query_sync(Ref, Endpoint, Message) ->
	gen_server:call(Ref, {send_query, Endpoint, Message}, infinity).

-spec stop(ref()) -> ok.
stop(Ref) -> gen_server:stop(Ref).

% gen_server

init(#{
	address := OverlayAddress,
	max_address := MaxAddress,
	transport := {TransportMod, TransportOpts}
} = Opts) ->
	case horde_transport:open(TransportMod, #{
		overlay_address => OverlayAddress,
		transport_opts => TransportOpts
	}) of
		{ok, Transport} ->
			horde_transport:recv_async(Transport),
			RingCheckInterval = maps:get(ring_check_interval, Opts, 60000),
			RingCheckTimer = ?TIME:start_timer(RingCheckInterval, self(), check_ring),
			RevFun = fun(Addr) when is_integer(Addr) -> MaxAddress - Addr end,
			State = #state{
				address = OverlayAddress,
				max_address = MaxAddress,
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
			{ok, State};
		{error, Reason} ->
			{stop, Reason}
	end.

handle_call(
	wait_join, From,
	#state{status = Status, join_waiters = Waiters} = State
) ->
	case Status of
		standalone ->
			{reply, false, State};
		ready ->
			{reply, true, State};
		_ ->
			{noreply, State#state{join_waiters = [From | Waiters]}}
	end;
handle_call(
	{lookup, Address, Tracer}, From,
	#state{
		address = OwnAddress,
		ring = Ring,
		num_parallel_queries = NumParallelQueries,
		num_neighbours = NumNeighbours
	} = State
) ->
	Peers = nodeset([
		horde_ring:successors(Address, 1, Ring),
		horde_ring:predecessors(Address, NumParallelQueries - 1, Ring),
		horde_ring:successors(OwnAddress, NumNeighbours, Ring),
		horde_ring:predecessors(OwnAddress, NumNeighbours, Ring)
	]),
	EndpointsOfPeers = [{compound, Addr} || #{address := Addr} <- Peers],
	_ = start_lookup(Address, EndpointsOfPeers, Tracer, {sync, From}, State),
	{noreply, State};
handle_call({send_query, Endpoint, Message}, {_Pid, QueryRef} = From, State) ->
	State2 = start_query(QueryRef, {sync, From}, Endpoint, Message, State),
	{noreply, State2};
handle_call({query_info, QueryRef}, _From, #state{queries = Queries} = State) ->
	Result =
		case maps:find(QueryRef, Queries) of
			{ok, Query} ->
				record_to_map(record_info(fields, query), Query);
			error ->
				undefined
		end,
	{reply, Result, State};
handle_call({info, What}, _From, State) ->
	{reply, extract_info(What, State), State}.

handle_cast(
	{join, BootstrapNodes},
	#state{address = OwnAddress, status = standalone} = State
) ->
	Tracer = horde_tracer:noop(),
	case start_lookup(OwnAddress, BootstrapNodes, Tracer, none, State) of
		{ok, Pid} ->
			MonitorRef = erlang:monitor(process, Pid),
			{noreply, State#state{status = {joining, MonitorRef}}};
		{error, Reason} ->
			error_logger:error_report([
				"Error while trying to join",
				{module, ?MODULE},
				{error, Reason}
			]),
			{noreply, State}
	end;
handle_cast({join, _}, State) ->
	{noreply, State};
handle_cast({send_query, Ref, Sender, Endpoint, Message}, State) ->
	State2 = start_query(Ref, {async, {Sender, Ref}}, Endpoint, Message, State),
	{noreply, State2}.

handle_info(Msg, State) ->
	{noreply, check_join_status(handle_info1(Msg, State))}.

terminate(_Reason, #state{transport = Transport}) ->
	_ = horde_transport:close(Transport),
	ok.

% Private

start_lookup(_, [], _Tracer, ReplyTo, _State) ->
	horde_utils:maybe_reply(?MODULE, ReplyTo, error),
	{error, no_resolver};
start_lookup(
	Address, Resolvers, Tracer, ReplyTo,
	#state{
		num_parallel_queries = NumParallelQueries,
		max_address = MaxAddress
	}
) ->
	LookupArgs = #{
		horde => self(),
		sender => ReplyTo,
		address => Address,
		resolvers => Resolvers,
		max_address => MaxAddress,
		num_parallel_queries => NumParallelQueries,
		tracer => Tracer
	},
	horde_lookup:start_link(LookupArgs).

maybe_ping(Endpoint, #state{queries = Queries} = State) ->
	IsQueryingAddress = lists:any(
		fun(#query{destination = Destination}) ->
			Destination =:= Endpoint
		end,
		maps:values(Queries)
	),
	case IsQueryingAddress of
		true ->
			State;
		false ->
			start_query(make_ref(), none, Endpoint, ping, State)
	end.

handle_overlay_message(
	#{from := Address, timestamp := Timestamp} = Header, Body,
	State
) ->
	State2 = handle_overlay_message1(Header, Body, State),
	Node = #{address => Address, last_seen => Timestamp, source => direct},
	add_node(Node, State2).

handle_overlay_message1(
	Header, {lookup, OwnAddress},
	#state{
		address = OwnAddress,
		transport = Transport,
		successor = OwnSuccessor,
		predecessor = OwnPredecessor
	} = State
) ->
	% If own address is being queried, reply with info and immediate neighbours
	TransportAddress = horde_transport:info(Transport, address),
	NodeInfo = #{
		address => #{overlay => OwnAddress, transport => TransportAddress},
		last_seen => erlang:monotonic_time(seconds)
	},
	Nodes = nodeset([[NodeInfo], set_of(OwnSuccessor), set_of(OwnPredecessor)]),
	reply(Header, {peer_info, Nodes}, State);
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
			nodeset([[Node], set_of(OwnPredecessor)]);
		% If node is not known, reply with a closer immediate neighbour and
		% a number of "next best hops" around the target.
		none ->
			CloserNeighbour =
				case horde_utils:is_address_between(
					OwnAddress,
					overlay_address(Sender),
					TargetAddress
				) of
					true -> OwnSuccessor;
					false -> OwnPredecessor
				end,
			nodeset([
				set_of(CloserNeighbour),
				horde_ring:successors(TargetAddress, 1, Ring),
				horde_ring:predecessors(TargetAddress, NumNextHops - 1, Ring)
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
	Nodes = nodeset([
		horde_ring:successors(OwnAddress, NumNeighbours, Ring),
		horde_ring:predecessors(OwnAddress, NumNeighbours, Ring)
	]),
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
		{ok, Query} ->
			Handler(Event, Id, Query, State);
		error ->
			State
	end.

handle_query_reply(
	{#{from := #{transport := TransportAddress} = CompoundAddress}, Message},
	Id, #query{sender = Sender, destination = Destination, timer = Timer},
	#state{queries = Queries} = State
) ->
	% Check whether reply comes from the queried address
	IsValidReply =
		case Destination of
			{compound, CompoundAddress} -> true;
			{transport, TransportAddress} -> true;
			_ -> false
		end,
	case IsValidReply of
		true ->
			_ = horde_utils:maybe_reply(?MODULE, Sender, {reply, Message}),
			_ = ?TIME:cancel_timer(Timer),
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
	_ = horde_utils:maybe_reply(?MODULE, Sender, noreply),
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
	do_send_query(Id, Query2, State).

handle_transport_error(
	Id, #query{timer = TimerRef} = Query,
	#state{queries = Queries, retry_delay = RetryDelay} = State
) ->
	_ = ?TIME:cancel_timer(TimerRef),
	Query2 = Query#query{
		timer = ?TIME:start_timer(RetryDelay, self(), {query_timeout, Id})
	},
	State#state{
		queries = Queries#{Id := Query2}
	}.

start_query(
	Ref, Sender, Destination, Message,
	#state{
		num_retries = NumRetries,
		num_nodes_probed = NumNodesProbed
	} = State
) ->
	Query = #query{
		sender = Sender,
		destination = Destination,
		message = Message,
		num_retries = NumRetries
	},
	State2 = do_send_query(Ref, Query, State),
	State2#state{num_nodes_probed = NumNodesProbed + 1}.

do_send_query(
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
		timer = ?TIME:start_timer(QueryTimeout, self(), {query_timeout, Id})
	},
	State#state{
		queries = Queries#{Id => Query2}
	}.

add_node(
	#{address := #{overlay := OwnAddress}},
	#state{address = OwnAddress} = State
) ->
	State;
add_node(Node, #state{ring = Ring} = State) ->
	State2 = State#state{ring = maybe_update_ring(Node, Ring)},
	State3 = maybe_update_neighbour(predecessor, Node, State2),
	maybe_update_neighbour(successor, Node, State3).

maybe_update_ring(#{last_seen := Timestamp} = Node, Ring) ->
	OverlayAddress = overlay_address(Node),
	case horde_ring:lookup(OverlayAddress, Ring) of
		{value, #{last_seen := CacheTimestamp}} when CacheTimestamp < Timestamp ->
			horde_ring:update(OverlayAddress, Node, Ring);
		none ->
			horde_ring:insert(OverlayAddress, Node, Ring);
		_ ->
			Ring
	end.

maybe_update_neighbour(_, [], State) ->
	State;
maybe_update_neighbour(Position, [Node], State) ->
	maybe_update_neighbour(Position, Node, State);
maybe_update_neighbour(
	predecessor, Node, #state{predecessor = undefined} = State
) ->
	maybe_set_neighbour(predecessor, Node, State);
maybe_update_neighbour(
	successor, Node, #state{successor = undefined} = State
) ->
	maybe_set_neighbour(successor, Node, State);
maybe_update_neighbour(
	successor,
	#{address := Address, last_seen := NewTimestamp},
	#state{
		successor = #{
			address := Address,
			last_seen := OldTimestamp
		} = Successor
	} = State
) when NewTimestamp > OldTimestamp ->
	State#state{successor = Successor#{last_seen := NewTimestamp}};
maybe_update_neighbour(
	predecessor,
	#{address := Address, last_seen := NewTimestamp},
	#state{
		predecessor = #{
			address := Address,
			last_seen := OldTimestamp
		} = Predecessor
	} = State
) when NewTimestamp > OldTimestamp ->
	State#state{predecessor = Predecessor#{last_seen := NewTimestamp}};
maybe_update_neighbour(
	predecessor, Node,
	#state{address = OwnAddress, predecessor = Predecessor} = State
) ->
	case horde_utils:is_address_between(
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
	case horde_utils:is_address_between(
		overlay_address(Node),
		OwnAddress,
		overlay_address(Successor)
	) of
		true -> maybe_set_neighbour(successor, Node, State);
		false -> State
	end.

maybe_set_neighbour(_, #{address := Address, source := indirect}, State) ->
	maybe_ping({compound, Address}, State);
maybe_set_neighbour(successor, #{source := direct} = Node, State) ->
	State#state{successor = Node};
maybe_set_neighbour(predecessor, #{source := direct} = Node, State) ->
	State#state{predecessor = Node}.

handle_info1(
	{'DOWN', BootstrapRef, process, _, normal},
	#state{status = {joining, BootstrapRef}} = State
) ->
	State#state{status = joining2};
handle_info1(
	{timeout, RingCheckTimer, check_ring},
	#state{
		ring_check_timer = RingCheckTimer,
		ring_check_interval = RingCheckInterval
	} = State
) ->
	State2 = check_ring(State),
	NewRingCheckTimer = ?TIME:start_timer(RingCheckInterval, self(), check_ring),
	State2#state{ring_check_timer = NewRingCheckTimer};
handle_info1({timeout, _, {query_timeout, Id}} = Event, State) ->
	dispatch_query_event(Event, fun handle_query_error/4, Id, State);
handle_info1(
	{horde_transport, Transport, {message, Header, Body}},
	#state{transport = Transport} = State
) ->
	State2 = handle_overlay_message(Header, Body, State),
	horde_transport:recv_async(Transport),
	State2;
handle_info1(
	{horde_transport, Transport, {transport_error, _, Id} = Error},
	#state{transport = Transport} = State
) ->
	dispatch_query_event(Error, fun handle_query_error/4, Id, State);
handle_info1(Msg, State) ->
	error_logger:warning_report([
		"Unexpected message", {module, ?MODULE}, {message, Msg}
	]),
	State.

check_join_status(#state{status = OldStatus} = State) ->
	NewStatus = join_status(State),
	case NewStatus =/= OldStatus of
		true ->
			notify_join_status(State#state{status = NewStatus});
		false ->
			State
	end.

join_status(
	#state{
		status = standalone,
		successor = Successor,
		predecessor = Predecessor
	}
) when Successor =/= undefined, Predecessor =/= undefined ->
	% Passive join
	ready;
join_status(
	#state{
		status = joining2 = Status,
		queries = Queries,
		successor = Successor,
		predecessor = Predecessor
	}
) ->
	% Active join
	NumPings = lists:foldl(
		fun(#query{message = Message}, Acc) ->
			case Message of
				ping -> Acc + 1;
				_ -> Acc
			end
		end,
		0,
		maps:values(Queries)
	),
	FinishedBootstrapping = NumPings =:= 0,
	FoundNeighbours = Successor =/= undefined andalso Predecessor =/= undefined,
	if
		FinishedBootstrapping and FoundNeighbours ->
			ready;
		FinishedBootstrapping and not FoundNeighbours ->
			standalone;
		true ->
			Status
	end;
join_status(
	#state{
		status = ready = Status,
		ring = Ring,
		queries = Queries,
		successor = Successor,
		predecessor = Predecessor
	}
) ->
	% Terrible network condition
	NumQueries = maps:size(Queries),
	RingSize = horde_ring:size(Ring),
	LeftHorde = true
		andalso NumQueries =:= 0
		andalso RingSize =:= 0
		andalso Successor =:= undefined
		andalso Predecessor =:= undefined,
	case LeftHorde of
		true -> standalone;
		false -> Status
	end;
join_status(#state{status = Status}) ->
	Status.

notify_join_status(#state{status = Status, join_waiters = Waiters} = State) ->
	_ = [gen_server:reply(Waiter, Status =:= ready) || Waiter <- Waiters],
	State#state{join_waiters = []}.

check_ring(
	#state{
		successor = Successor,
		predecessor = Predecessor
	} = State
) ->
	State2 = maybe_ping_neighbour(Successor, State),
	maybe_ping_neighbour(Predecessor, State2).

maybe_ping_neighbour(undefined, State) ->
	State;
maybe_ping_neighbour(#{address := Address}, State) ->
	maybe_ping({compound, Address}, State).

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
	Ring2 = horde_ring:remove(overlay_address(RemovedAddress), Ring),
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

extract_info(queries, #state{queries = Queries}) ->
	maps:map(
		fun(_, V) ->
			record_to_map(record_info(fields, query), V)
		end,
		Queries
	);
extract_info(status, #state{status = Status}) ->
	case Status of
		ready -> ready;
		standalone -> standalone;
		{joining, _} -> joining;
		joining2 -> joining
	end;
extract_info(
	address, #state{address = OverlayAddress, transport = Transport}
) ->
	TransportAddress = horde_transport:info(Transport, address),
	#{overlay => OverlayAddress, transport => TransportAddress};
extract_info(What, State) ->
	maps:get(What, record_to_map(record_info(fields, state), State)).

nodeset(Sets) ->
	maps:values(
		lists:foldl(fun nodeset_add_set/2, #{}, Sets)
	).

nodeset_add_set(Set, Nodes) ->
	lists:foldl(fun nodeset_add_node/2, Nodes, Set).

nodeset_add_node(
	#{address := #{overlay := Address},
	  last_seen := Timestamp} = Node,
	Nodes
) ->
	StrippedNode = maps:remove(source, Node),
	case maps:find(Address, Nodes) of
		{ok, #{last_seen := OldTimestamp}} when OldTimestamp < Timestamp ->
			Nodes#{Address := StrippedNode};
		error ->
			Nodes#{Address => StrippedNode};
		_ ->
			Nodes
	end.

record_to_map(Fields, Record) ->
	maps:from_list(lists:zip(Fields, tl(tuple_to_list(Record)))).

undefined_if_removed(Address, #{address := Address}) -> undefined;
undefined_if_removed(_, Node) -> Node.

overlay_address(#{address := #{overlay := Address}}) -> Address;
overlay_address(#{overlay := Address}) -> Address.

set_of(undefined) -> [];
set_of(Element) -> [Element].
