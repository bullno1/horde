-module(horde).
-behaviour(gen_server).
% API
-export([
	default_realm/0,
	generate_keypair/1,
	start_link/1,
	start_link/2,
	lookup/3,
	send/3
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
	node_info/0,
	txid/0,
	message/0,
	message_header/0,
	message_body/0
]).

-define(MAX_TXID, 65535).

-type overlay_address() :: non_neg_integer().
-type compound_address() :: #{
	overlay := overlay_address(),
	transport := horde_transport:address()
}.
-type message() :: {message_header(), message_body()}.
-type txid() :: 0..?MAX_TXID.
-type message_header() :: #{
	from := compound_address(),
	txid := txid(),
	timestamp := non_neg_integer()
}.
-type node_info() :: #{
	address := compound_address(),
	last_seen := non_neg_integer(),
	source => direct | indirect
}.
-type message_body() ::
	  {lookup, overlay_address()}
	| ping
	| {peer_info, [node_info()]}
	| {user, binary()}.
-type name() :: {local, atom()} | {via, module(), term()}.
-type ref() :: pid() | atom() | {via, module(), term()}.
-type realm() :: #{
	name := binary(),
	crypto := {module(), term()}
}.
-type opts() :: #{
	realm := realm(),
	keypair := horde_crypto:keypair(),
	transport := {Module :: module(), Opts :: term()},
	bootstrap_nodes => [horde_transport:address()],
	ring_check_interval => non_neg_integer(),
	num_parallel_queries => pos_integer(),
	num_next_hops => pos_integer(),
	num_neighbours => pos_integer(),
	min_slice_size => pos_integer()
}.
-type query_target()
	:: {transport, horde_transport:address()}
	 | {compound, compound_address()}.

-record(query_status, {
	num_retries :: non_neg_integer()
}).
-record(lookup, {
	address :: overlay_address(),
	reply_to :: {client, term()} | none,
	queries :: #{query_target() => #query_status{}}
}).
-record(ping, {
	address :: compound_address(),
	num_retries :: non_neg_integer()
}).
-record(state, {
	realm_name :: binary(),
	crypto :: horde_crypto:ctx(),
	address :: compound_address(),
	keypair :: horde_crypto:keypair(),
	transport :: horde_transport:ref(),
	next_txid = 0 :: txid(),
	transactions = #{} :: #{txid() => #lookup{} | #ping{}},
	successor :: node_info() | undefined,
	predecessor :: node_info() | undefined,
	ring :: horde_ring:ring(overlay_address(), node_info()),
	ring_check_timer :: reference(),
	ring_check_interval :: non_neg_integer(),
	num_nodes_probed = 0 :: non_neg_integer(),
	num_nodes_failed = 0 :: non_neg_integer(),
	query_timeout :: pos_integer(),
	num_retries :: pos_integer(),
	num_parallel_queries :: pos_integer(),
	num_next_hops :: pos_integer(),
	num_neighbours :: pos_integer(),
	min_slice_size :: pos_integer()
}).

% API

-spec default_realm() -> realm().
default_realm() ->
	#{
		name => <<"horde">>,
		crypto => {horde_ecdsa, #{
			hash_algo => sha256,
			address_size => 160,
			curve => secp521r1
		}}
	}.

-spec generate_keypair(realm()) -> horde_crypto:keypair().
generate_keypair(#{crypto := {Module, Opts}}) ->
	Ctx = horde_crypto:init(Module, Opts),
	horde_crypto:generate_keypair(Ctx).

-spec start_link(opts()) -> {ok, pid()} | {error, any()}.
start_link(Opts) -> gen_server:start_link(?MODULE, Opts, []).

-spec start_link(name(), opts()) -> {ok, pid()} | {error, any()}.
start_link(Name, Opts) -> gen_server:start_link(Name, ?MODULE, Opts, []).

-spec lookup(ref(), overlay_address(), timeout())
	-> {ok, horde_transport:address()}
	 | {error, term()}.
lookup(Ref, Address, Timeout) ->
	gen_server:call(Ref, {lookup, Address}, Timeout).

-spec send(ref(), overlay_address(), binary()) -> ok | {error, term()}.
send(Ref, Dest, Message) ->
	gen_server:call(Ref, {send, Dest, Message}).

% gen_server

init(#{
	realm := #{
		name := RealmName,
		crypto := {CryptoMod, CryptoOpts}
	},
	bootstrap_nodes := BootstrapNodes,
	keypair := {PubKey, _} = KeyPair,
	transport := {TransportMod, TransportOpts}
} = Opts) ->
	case horde_transport:open(TransportMod, TransportOpts) of
		{ok, Transport} ->
			Crypto = horde_crypto:init(CryptoMod, CryptoOpts),
			OverlayAddress = horde_crypto:address_of(Crypto, PubKey),
			TransportAddress = horde_transport:info(Transport, address),
			RingCheckInterval = maps:get(ring_check_interval, Opts, 60000),
			RingCheckTimer = erlang:send_after(RingCheckInterval, self(), check_ring),
			MaxAddress = horde_crypto:info(Crypto, max_address),
			RevFun = fun(Addr) -> MaxAddress - Addr end,
			State = #state{
				realm_name = RealmName,
				crypto = Crypto,
				address = #{
					overlay => OverlayAddress,
					transport => TransportAddress
				},
				keypair = KeyPair,
				transport = Transport,
				ring = horde_ring:new(RevFun),
				ring_check_timer = RingCheckTimer,
				ring_check_interval = RingCheckInterval,
				query_timeout = maps:get(query_timeout, Opts, 5000),
				num_retries = maps:get(num_retries, Opts, 3),
				num_parallel_queries = maps:get(k, Opts, 3),
				num_next_hops = maps:get(k, Opts, 3),
				num_neighbours = maps:get(k, Opts, 5),
				min_slice_size = maps:get(j, Opts, 2)
			},
			% Bootstrap
			LookupTargets = [{transport, Address} || Address <- BootstrapNodes],
			State2 = send_lookup(OverlayAddress, LookupTargets, none, State),
			{ok, State2};
		{error, Reason} ->
			{stop, Reason}
	end.

handle_call(
	{lookup, Address}, From,
	#state{ring = Ring, num_parallel_queries = NumParallelQueries} = State
) ->
	Peers = ordsets:union(
		horde_ring:successors(Address, 1, Ring),
		horde_ring:predecessors(Address, NumParallelQueries - 1, Ring)
	),
	Targets = [{compound, Addr} || #{address := Addr} <- Peers],
	{noreply, send_lookup(Address, Targets, {client, From}, State)}.

handle_cast(_, State) -> {noreply, State}.

handle_info(
	{query_timeout, Txid, Target},
	#state{transactions = Transactions} = State
) ->
	State2 = case maps:find(Txid, Transactions) of
		{ok, Transaction} ->
			handle_tx_event({query_timeout, Target}, Txid, Transaction, State);
		error ->
			State
	end,
	{noreply, State2};
handle_info({horde_transport, Msg}, State) ->
	{noreply, handle_overlay_message(Msg, State)};
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

send_lookup(
	Address, Targets, ReplyTo,
	#state{
		next_txid = Txid,
		num_retries = NumRetries,
		num_nodes_probed = NumNodesProbed,
		transactions = Transactions
	} = State
) ->
	% Send lookup
	State2 = lists:foldl(
		fun(Target, StateAcc) ->
			send_query(Txid, Target, {lookup, Address}, StateAcc)
		end,
		State,
		Targets
	),
	% Track lookup
	Queries = maps:from_list([
		{Target, #query_status{num_retries = NumRetries}} || Target <- Targets
	]),
	Lookup = #lookup{address = Address, reply_to = ReplyTo, queries = Queries},
	State2#state{
		next_txid = (Txid + 1) rem ?MAX_TXID,
		transactions = Transactions#{Txid => Lookup},
		num_nodes_probed = NumNodesProbed + length(Targets)
	}.

handle_overlay_message(
	{#{from := Address, timestamp := Timestamp} = Header, Body},
	State
) ->
	State2 = handle_overlay_message(Header, Body, State),
	Node = #{address => Address, last_seen => Timestamp, source => direct},
	add_node(Node, State2).

handle_overlay_message(
	#{from := Sender} = Header, {lookup, Address},
	#state{
		ring = Ring,
		address = OwnAddress,
		successor = OwnSuccessor,
		predecessor = OwnPredecessor,
		num_next_hops = NumNextHops
	} = State
) ->
	Nodes = case horde_ring:lookup(Address, Ring) of
		% If node is known, reply with info and own immediate predecessor
		{value, Node} ->
			ordsets:add_element(Node, set_of(OwnPredecessor));
		none ->
			% l next best hops for x are the hops immediately after x and l - 1
			% predecessors.
			NextBestHops = ordsets:union(
				horde_ring:successors(Address, 1, Ring),
				horde_ring:predecessors(Address, NumNextHops - 1, Ring)
			),
			ExtraNode =
				case horde_address:is_between(
					overlay_address(OwnAddress), overlay_address(Sender), Address
				) of
					true -> OwnSuccessor;
					false -> OwnPredecessor
				end,
			ordsets:union(set_of(ExtraNode), NextBestHops)
	end,
	reply(Header, {peer_info, Nodes}, State);
handle_overlay_message(
	Header, ping,
	#state{
		ring = Ring, address = #{overlay := OwnAddress},
		num_neighbours = NumNeighbours
	} = State
) ->
	Nodes = ordsets:union(
		horde_ring:successors(OwnAddress, NumNeighbours, Ring),
		horde_ring:predecessors(OwnAddress, NumNeighbours, Ring)
	),
	reply(Header, {peer_info, Nodes}, State);
handle_overlay_message(
	#{txid := Txid, address := Address},
	{peer_info, Peers},
	#state{transactions = Transactions} = State
) ->
	State2 = case maps:find(Txid, Transactions) of
		{ok, Transaction} ->
			handle_tx_event({result, Address, Peers}, Txid, Transaction, State);
		_ ->
			State
	end,
	lists:foldl(
		fun(Peer, StateAcc) ->
			add_node(Peer#{source => indirect}, StateAcc)
		end,
		State2,
		Peers
	).

reply(
	#{from := #{transport := SenderAddress}, txid := Txid},
	MsgBody,
	#state{transport = Transport, address = OwnAddress} = State
) ->
	Header = #{
		from => OwnAddress,
		txid => Txid,
		timestamp => erlang:system_time(seconds)
	},
	horde_transport:send(Transport, SenderAddress, {Header, MsgBody}),
	State.

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

remove_node({transport, _}, State) ->
	State;
remove_node(
	{compound, RemovedAddress},
	#state{
		address = #{overlay := OwnAddress},
		ring = Ring,
		predecessor = Predecessor,
		successor = Successor
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

maybe_update_neighbour(_, [], State) ->
	State;
maybe_update_neighbour(Position, [Node], State) ->
	maybe_update_neighbour(Position, Node, State);
maybe_update_neighbour(
	predecessor, Node, #state{predecessor = undefined} = State
) when is_map(Node) ->
	State#state{predecessor = Node};
maybe_update_neighbour(
	successor, Node, #state{successor = undefined} = State
) when is_map(Node) ->
	State#state{successor = Node};
maybe_update_neighbour(
	predecessor, #{source := Source} = Node,
	#state{address = OwnAddress, predecessor = Predecessor} = State
) ->
	case horde_address:is_between(
		overlay_address(Node),
		overlay_address(Predecessor),
		overlay_address(OwnAddress)
	) of
		true ->
			case Source of
				direct -> State#state{predecessor = Node};
				indirect -> start_ping(Node, State)
			end;
		false ->
			State
	end;
maybe_update_neighbour(
	successor, #{source := Source} = Node,
	#state{address = OwnAddress, successor = Successor} = State
) ->
	case horde_address:is_between(
		overlay_address(Node), overlay_address(OwnAddress), overlay_address(Successor)
	) of
		true ->
			case Source of
				direct -> State#state{successor = Node};
				indirect -> start_ping(Node, State)
			end;
		false ->
			State
	end.

undefined_if_removed(Address, #{address := Address}) -> undefined;
undefined_if_removed(_, Node) -> Node.

start_ping(Node, State) ->
	case is_pinging(overlay_address(Node), State#state.transactions) of
		true -> State;
		false -> start_ping1(Node, State)
	end.

is_pinging(Address, Transactions) ->
	lists:any(
		fun(Transaction) -> is_targeting(Address, Transaction) end,
		maps:values(Transactions)
	).

is_targeting(Address, #ping{address = #{overlay := Address}}) ->
	true;
is_targeting(Address, #lookup{queries = Queries}) ->
	lists:any(
		fun({compound, CompoundAddress}) ->
			overlay_address(CompoundAddress) =:= Address;
		   (_) ->
			false
		end,
		maps:keys(Queries)
	).

start_ping1(
	#{address := Address},
	#state{
		next_txid = Txid,
		transactions = Transactions,
		num_nodes_probed = NumNodesProbed,
		num_retries = NumRetries
	} = State
) ->
	% Send ping
	State2 = send_query(Txid, {compound, Address}, ping, State),
	% Track ping
	Ping = #ping{address = Address, num_retries = NumRetries},
	State2 = State#state{
		next_txid = (Txid + 1) rem ?MAX_TXID,
		transactions = Transactions#{Txid => Ping},
		num_nodes_probed = NumNodesProbed + 1
	}.

send_query(
	Txid, Target, Message,
	#state{
	    address = OwnAddress,
	    transport = Transport,
		query_timeout = QueryTimeout
	} = State
) ->
	Header = #{
		from => OwnAddress,
		txid => Txid,
		timestamp => erlang:system_time(seconds)
	},
	horde_transport:send(Transport, transport_address(Target), {Header, Message}),
	_ = erlang:send_after(QueryTimeout, self(), {query_timeout, Txid, Target}),
	State.

handle_tx_event(Event, Txid, #ping{} = Ping, State) ->
	handle_ping_event(Event, Txid, Ping, State);
handle_tx_event(Event, Txid, #lookup{} = Lookup, State) ->
	handle_lookup_event(Event, Txid, Lookup, State).

handle_ping_event(
	{query_timeout, Target},
	Txid, #ping{num_retries = 0},
	#state{
		transactions = Transactions,
		num_nodes_failed = NumNodesFailed
	} = State
) ->
	State2 = State#state{
		transactions = maps:remove(Txid, Transactions),
		num_nodes_failed = NumNodesFailed + 1
	},
	remove_node(Target, State2);
handle_ping_event(
	{query_timeout, Target},
	Txid, #ping{num_retries = NumRetries} = Ping,
	#state{transactions = Transactions} = State
) ->
	State2 = State#state{
		transactions = Transactions#{
			Txid := Ping#ping{num_retries = NumRetries - 1}
		}
	},
	send_query(Txid, Target, ping, State2);
handle_ping_event(
	{result, Address, _},
	Txid, #ping{address = Address},
	#state{transactions = Transactions} = State
) ->
	State#state{transactions = maps:remove(Txid, Transactions)};
handle_ping_event(_Event, _Txid, _Ping, State) ->
	State.

handle_lookup_event(
	{query_timeout, Target}, Txid, #lookup{queries = Queries} = Lookup, State
) ->
	case maps:find(Target, Queries) of
		{ok, Query} ->
			handle_lookup_query_timeout(Target, Query, Txid, Lookup, State);
		error ->
			State
	end.

handle_lookup_query_timeout(
	Target, #query_status{num_retries = NumRetries} = Query,
	Txid, #lookup{
		address = Address, queries = Queries, reply_to = Client
	} = Lookup,
	#state{
		transactions = Transactions,
		num_nodes_failed = NumNodesFailed
	} = State
) ->
	NextAction = case NumRetries > 0 of
		true -> {retry, Query#query_status{num_retries = NumRetries - 1}};
		false -> stop
	end,
	case NextAction of
		{retry, Query2} ->
			Queries2 = Queries#{Target := Query2},
			Transactions2 = Transactions#{
				Txid := Lookup#lookup{queries = Queries2}
			},
			State2 = State#state{transactions = Transactions2},
			send_query(Txid, Target, {lookup, Address}, State2);
		stop ->
			Queries2 = maps:remove(Target, Queries),
			Transactions2 = case maps:size(Queries2) =:= 0 of
				true ->
					maybe_reply(Client, {error, timeout}),
					maps:remove(Txid, Transactions);
				false ->
					Transactions#{Txid := Lookup#lookup{queries = Queries2}}
			end,
			State2 = State#state{
				transactions = Transactions2,
				num_nodes_failed = NumNodesFailed + 1
			},
			remove_node(Target, State2)
	end.

maybe_reply(none, _) -> ok;
maybe_reply({_, _} = Client, Msg) -> gen_server:reply(Client, Msg).

overlay_address(#{address := #{overlay := Address}}) -> Address;
overlay_address(#{overlay := Address}) -> Address.

transport_address({transport, Address}) -> Address;
transport_address({compound, #{transport := Address}}) -> Address.

set_of(undefined) -> [];
set_of(Element) -> [Element].
