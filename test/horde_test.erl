-module(horde_test).
-compile(export_all).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-record(state, {
	bootstrap_node,
	nodes = []
}).

% Property

prop_test_() ->
	?_assert(proper:quickcheck(prop_horde(), [{to_file, user}])).

prop_horde() ->
	?FORALL(Commands, commands(?MODULE),
		begin
			meck:new([horde_mock], [non_strict, no_history]),
			meck:expect(horde_mock, start_timer,
				fun(_Timeout, _Dest, _Message) -> make_ref() end
			),
			meck:expect(horde_mock, cancel_timer, fun(_) -> ok end),

			{History, State, Result} = run_commands(?MODULE, Commands),
			_ = [horde:stop(Node) || Node <- State#state.nodes],

			meck:unload([horde_mock]),

			?WHENFAIL(
				io:format(
					user,
					"History: ~p\nState: ~p\nResult: ~p\n",
					[History, format_state(State), Result]
				),
				aggregate(command_names(Commands), Result =:= ok)
			)
		end).

% Model

initial_state() -> #state{}.

command(#state{bootstrap_node = undefined}) ->
	{call, ?MODULE, new_node, []};
command(#state{nodes = Nodes, bootstrap_node = BootstrapNode}) ->
	oneof([
		{call, ?MODULE, new_node, []},
		?LET(Node, oneof(Nodes),
			{call, ?MODULE, node_join, [Node, BootstrapNode]})
	]).

precondition(_, {call, ?MODULE, node_join, [Node, BootstrapNode]}) ->
	Node =/= BootstrapNode;
precondition(_, {call, _Mod, _Fun, _Args}) ->
	true.

postcondition(
	#state{nodes = Nodes},
	{call, ?MODULE, new_node, []},
	NewNode
) ->
	#{overlay := NodeAddress} = horde:info(NewNode, address),
	horde:info(NewNode, status) =:= standalone
	andalso lists:all(
		fun(Node) ->
			not (Node =/= NewNode andalso horde:info(Node, status) =:= ready)
			orelse
			error =:= horde:lookup(Node, NodeAddress, infinity)
		end,
		Nodes
	);
postcondition(
	#state{nodes = Nodes},
	{call, ?MODULE, node_join, [JoinedNode, _]},
	Joined
) ->
	#{transport := TransportAddress, overlay := OverlayAddress} =
		horde:info(JoinedNode, address),
	Joined
	andalso horde:info(JoinedNode, status) =:= ready
	andalso lists:all(
		fun(Node) ->
			not (horde:info(Node, status) =:= ready)
			orelse
			{ok, TransportAddress} =:= horde:lookup(Node, OverlayAddress, infinity)
		end,
		Nodes
	);
postcondition(_State, {call, _Mod, _Fun, _Args}, _Res) ->
	true.

next_state(
	State,
	_,
	{call, ?MODULE, node_join, [_, _]}
) ->
	State;
next_state(
	#state{bootstrap_node = undefined} = State,
	Node,
	{call, ?MODULE, new_node, []}
) ->
	State#state{bootstrap_node = Node, nodes = [Node]};
next_state(
	#state{nodes = Nodes} = State,
	Node,
	{call, ?MODULE, new_node, []}
) ->
	State#state{nodes = [Node | Nodes]}.

% Helpers

format_state(State) ->
	maps:from_list(
		lists:zip(
			record_info(fields, state),
			tl(tuple_to_list(State))
		)
	).

new_node() ->
	Crypto = horde_crypto:default(),
	Opts = #{
		crypto => Crypto,
		keypair => horde_crypto:generate_keypair(Crypto),
		transport => {horde_disterl, #{active => false}}
	},
	{ok, Node} = horde:start_link(Opts),
	Node.

node_join(Node, BootstrapNode) ->
	#{transport := TransportAddress} = horde:info(BootstrapNode, address),
	horde:join(Node, [{transport, TransportAddress}], infinity).
