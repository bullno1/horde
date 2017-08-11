-module(horde).
-behaviour(gen_server).
% API
-export([
	start_link/1,
	start_link/2,
	watch/1,
	unwatch/2,
	lookup/2,
	lookup/3,
	info/2,
	join/2,
	wait_join/1,
	wait/2,
	ping/2,
	stop/1
]).
% Low-level API
-export([
	send_query/3,
	send_query_async/3,
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
-include("horde_utils.hrl").
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
	cache_period => non_neg_integer(),
	query_timeout => pos_integer(),
	num_retries => non_neg_integer(),
	retry_delay => non_neg_integer(),
	num_parallel_queries => pos_integer(),
	num_next_hops => pos_integer(),
	num_neighbours => pos_integer(),
	min_slice_size => pos_integer()
}.
-type lookup_opts() :: #{
	resolvers => [endpoint()],
	tracer => horde_tracer:tracer()
}.
-type query_info() :: #{
	sender := horde_utils:reply_to(),
	destination := endpoint(),
	message := message_body(),
	num_retries := non_neg_integer(),
	timer := reference() | undefined
}.
-type wait_cond(T) :: fun((ref()) -> {stop, T} | continue).

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
	queries = #{} :: #{reference() => #query{}},
	watchers = [] :: [{reference(), pid()}],
	successor :: node_info() | undefined,
	predecessor :: node_info() | undefined,
	ring :: horde_ring:ring(overlay_address(), node_info()),
	ring_check_timer :: reference(),
	ring_check_interval :: non_neg_integer(),
	cache_period :: non_neg_integer(),
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

-spec watch(ref()) -> reference().
watch(Ref) -> gen_server:call(Ref, watch).

-spec unwatch(ref(), reference()) -> ok.
unwatch(Ref, MonitorRef) -> gen_server:call(Ref, {unwatch, MonitorRef}).

-spec lookup(ref(), overlay_address())
	-> {ok, horde_transport:address()} | error.
lookup(Ref, Address) -> lookup(Ref, Address, #{}).

-spec lookup(ref(), overlay_address(), lookup_opts())
	-> {ok, horde_transport:address()} | error.
lookup(Ref, Address, LookupOpts) ->
	gen_server:call(Ref, {lookup, Address, LookupOpts}, infinity).

-spec info
	(ref(), status) -> standalone | joining | joined;
	(ref(), ring) -> horde_ring:ring(overlay_address(), node_info());
	(ref(), queries) -> #{reference() => query_info()};
	(ref(), successor) -> node_info() | undefined;
	(ref(), predecessor) -> node_info() | undefined;
	(ref(), address) -> compound_address();
	(ref(), transport) -> horde_transport:ref().
info(Ref, Info) ->
	gen_server:call(Ref, {info, Info}).

-spec query_info(ref(), reference()) -> query_info() | undefined.
query_info(Ref, QueryRef) -> gen_server:call(Ref,  {query_info, QueryRef}).

-spec join(ref(), [endpoint()]) -> boolean().
join(Ref, BootstrapNodes) ->
	OwnAddress = overlay_address(info(Ref, address)),
	_ = lookup(Ref, OwnAddress, #{resolvers => BootstrapNodes}),
	wait_join(Ref).

-spec ping(ref(), endpoint()) -> pong | pang.
ping(Ref, Endpoint) ->
	case send_query(Ref, Endpoint, ping) of
		{reply, _} -> pong;
		noreply -> pang
	end.

-spec wait_join(ref()) -> boolean().
wait_join(Ref) -> wait(Ref, fun wait_join_cond/1).

-spec wait(ref(), wait_cond(T)) -> T.
wait(Ref, WaitCond) ->
	WatchRef = watch(Ref),
	MonitorRef = monitor(process, Ref),
	try
		wait_loop(Ref, WatchRef, MonitorRef, WaitCond)
	after
		demonitor(MonitorRef, [flush]),
		catch unwatch(Ref, WatchRef)
	end.

-spec send_query_async(ref(), endpoint(), message_body()) -> reference().
send_query_async(Ref, Endpoint, Message) ->
	QueryRef = make_ref(),
	gen_server:cast(Ref, {send_query, QueryRef, self(), Endpoint, Message}),
	QueryRef.

-spec send_query(ref(), endpoint(), message_body()) ->
	{reply, message_body()} | noreply.
send_query(Ref, Endpoint, Message) ->
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
			CachePeriod = maps:get(cache_period, Opts, 120000),
			RingCheckTimer = ?TIME:start_timer(RingCheckInterval, self(), check_ring),
			State = #state{
				address = OverlayAddress,
				max_address = MaxAddress,
				transport = Transport,
				ring = horde_ring:new(rev_fun(MaxAddress)),
				ring_check_timer = RingCheckTimer,
				ring_check_interval = RingCheckInterval,
				cache_period = CachePeriod,
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
	{lookup, Address, Opts}, From,
	#state{
		address = OwnAddress,
		ring = Ring,
		num_parallel_queries = NumParallelQueries,
		num_neighbours = NumNeighbours,
		max_address = MaxAddress
	} = State
) ->
	BestPeers = nodeset([
		horde_ring:successors(Address, 1, Ring),
		horde_ring:predecessors(Address, NumParallelQueries - 1, Ring),
		horde_ring:successors(OwnAddress, NumNeighbours, Ring),
		horde_ring:predecessors(OwnAddress, NumNeighbours, Ring)
	]),
	LookupArgs = #{
		horde => self(),
		sender => {sync, From},
		address => Address,
		max_address => MaxAddress,
		num_parallel_queries => NumParallelQueries,
		resolvers => [{compound, Addr} || #{address := Addr} <- BestPeers],
		tracer => horde_tracer:noop()
	},
	_ = horde_lookup:start_link(maps:merge(LookupArgs, Opts)),
	{noreply, State};
handle_call(watch, {Pid, _}, #state{watchers = Watchers} = State) ->
	MonitorRef = erlang:monitor(process, Pid),
	{reply, MonitorRef, State#state{watchers = [{MonitorRef, Pid} | Watchers]}};
handle_call({unwatch, Ref}, _From, #state{watchers = Watchers} = State) ->
	erlang:demonitor(Ref, [flush]),
	{reply, ok, State#state{watchers = lists:keydelete(Ref, 1, Watchers)}};
handle_call({send_query, Endpoint, Message}, {_Pid, QueryRef} = From, State) ->
	State2 = start_query(QueryRef, {sync, From}, Endpoint, Message, State),
	{noreply, State2};
handle_call({query_info, QueryRef}, _From, #state{queries = Queries} = State) ->
	Result =
		case maps:find(QueryRef, Queries) of
			{ok, Query} ->
				?RECORD_TO_MAP(query, Query);
			error ->
				undefined
		end,
	{reply, Result, State};
handle_call({info, What}, _From, State) ->
	{reply, extract_info(What, State), State}.

handle_cast({send_query, Ref, Sender, Endpoint, Message}, State) ->
	State2 = start_query(Ref, {async, {Sender, Ref}}, Endpoint, Message, State),
	{noreply, State2}.

handle_info(
	{'DOWN', MonitorRef, process, _, _},
	#state{watchers = Watchers} = State
) ->
	{noreply, State#state{watchers = lists:keydelete(MonitorRef, 1, Watchers)}};
handle_info(
	{timeout, RingCheckTimer, check_ring},
	#state{
		ring_check_timer = RingCheckTimer,
		ring_check_interval = RingCheckInterval
	} = State
) ->
	State2 = check_ring(State),
	RingCheckTimer2 = ?TIME:start_timer(RingCheckInterval, self(), check_ring),
	{noreply, State2#state{ring_check_timer = RingCheckTimer2}};
handle_info({timeout, _, {query_timeout, Id}} = Event, State) ->
	State2 = dispatch_query_event(Event, fun handle_query_error/4, Id, State),
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
		"Unexpected message", {module, ?MODULE}, {message, Msg}
	]),
	{noreply, State}.

terminate(_Reason, #state{transport = Transport}) ->
	_ = horde_transport:close(Transport),
	ok.

% Private

rev_fun(MaxAddress) ->
	fun(Addr) when is_integer(Addr) -> MaxAddress - Addr end.

wait_loop(Ref, WatchRef, MonitorRef, WaitCond) ->
	case WaitCond(Ref) of
		{stop, Result} ->
			Result;
		wait ->
			receive
				{?MODULE, WatchRef, _} ->
					wait_loop(Ref, WatchRef, MonitorRef, WaitCond);
				{'DOWN', MonitorRef, process, _, Reason} ->
					exit(Reason)
			end
	end.

wait_join_cond(Ref) ->
	case info(Ref, status) of
		standalone -> {stop, false};
		joining -> wait;
		joined -> {stop, true}
	end.

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
		% If node is known, reply with info and neighbours
		{value, Node} ->
			nodeset([
				[Node],
				horde_ring:successors(TargetAddress, 1, Ring),
				horde_ring:predecessors(TargetAddress, 1, Ring)
			]);
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
	notify_watchers({found_node, Node}, State),
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
	case is_better_predecessor(
		overlay_address(Node),
		OwnAddress,
		overlay_address(Predecessor)
	) of
		true -> maybe_set_neighbour(predecessor, Node, State);
		false -> State
	end;
maybe_update_neighbour(
	successor, Node,
	#state{address = OwnAddress, successor = Successor} = State
) ->
	case is_better_successor(
		overlay_address(Node),
		OwnAddress,
		overlay_address(Successor)
	) of
		true -> maybe_set_neighbour(successor, Node, State);
		false -> State
	end.

is_better_successor(Address, OwnAddress, SuccessorAddress) ->
	horde_utils:is_address_between(Address, OwnAddress, SuccessorAddress).

is_better_predecessor(Address, OwnAddress, PredecessorAddress) ->
	horde_utils:is_address_between(Address, PredecessorAddress, OwnAddress).

maybe_set_neighbour(_, #{address := Address, source := indirect}, State) ->
	maybe_ping({compound, Address}, State);
maybe_set_neighbour(successor, #{source := direct} = Node, State) ->
	State#state{successor = Node};
maybe_set_neighbour(predecessor, #{source := direct} = Node, State) ->
	State#state{predecessor = Node}.

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
	notify_watchers({remove_node, RemovedAddress}, State),
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
		successor, horde_ring:successors(OwnAddress, 1, Ring2), State3
	).

notify_watchers(Msg, #state{watchers = Watchers}) ->
	_ = [Pid ! {?MODULE, Ref, Msg} || {Ref, Pid} <- Watchers],
	ok.

extract_info(status, #state{
	successor = Successor,
	predecessor = Predecessor,
	queries = Queries,
	address = OwnAddress
}) ->
	case (Successor =/= undefined) and (Predecessor =/= undefined) of
		true ->
			SuccessorAddress = overlay_address(Successor),
			PredecessorAddress = overlay_address(Predecessor),
			QueryInfos = maps:values(Queries),
			NoBetterNeighbours = lists:all(
				fun(#query{destination = {compound, CompoundAddress}}) ->
					OverlayAddress = overlay_address(CompoundAddress),
					not is_better_successor(
						OverlayAddress, OwnAddress, SuccessorAddress
					) andalso not is_better_predecessor(
						OverlayAddress, OwnAddress, PredecessorAddress
					);
				   (_) ->
					true
				end,
				QueryInfos
			),
			case NoBetterNeighbours of
				true -> joined;
				false -> joining
			end;
		false ->
			case maps:size(Queries) =:= 0 of
				true -> standalone;
				false -> joining
			end
	end;
extract_info(queries, #state{queries = Queries}) ->
	maps:map(fun(_, V) -> ?RECORD_TO_MAP(query, V) end, Queries);
extract_info(address, #state{
	address = OverlayAddress, transport = Transport
}) ->
	TransportAddress = horde_transport:info(Transport, address),
	#{overlay => OverlayAddress, transport => TransportAddress};
extract_info(What, State) ->
	maps:get(What, ?RECORD_TO_MAP(state, State)).

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

undefined_if_removed(Address, #{address := Address}) -> undefined;
undefined_if_removed(_, Node) -> Node.

overlay_address(#{address := #{overlay := Address}}) -> Address;
overlay_address(#{overlay := Address}) -> Address.

set_of(undefined) -> [];
set_of(Element) -> [Element].
