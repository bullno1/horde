-module(horde_prop).
-compile(export_all).
-include_lib("proper/include/proper.hrl").
-include_lib("stdlib/include/assert.hrl").
-record(state, {
	bootstrap_node,
	nodes = []
}).
-define(WHEN(A, B), case A of true -> B; false -> true end).

% Property

prop_horde() ->
	?TIMEOUT(10000,
		?FORALL(
			{Commands, S1, S2, S3},
			{commands(?MODULE),
			 non_neg_integer(), non_neg_integer(), non_neg_integer()},
			begin
				horde_fake_crypto:reset_seed([exs1024s, {S1, S2, S3}]),
				{History, State, Result} = run_commands(?MODULE, Commands),
				_ = [catch horde:stop(Node) || Node <- State#state.nodes],

				?WHENFAIL(
					begin
						format_history(History, Commands),
						io:format(user, "Last state: ~p~n", [format_state(State)]),
						io:format(user, "Result: ~p~n", [Result])
					end,
					aggregate(command_names(Commands), Result =:= ok)
				)
			end)).

% Model

initial_state() -> #state{}.

command(#state{bootstrap_node = undefined}) ->
	{call, ?MODULE, new_node, []};
command(#state{nodes = Nodes, bootstrap_node = BootstrapNode}) ->
	oneof([
		{call, ?MODULE, new_node, []},
		?LET(Node, oneof(Nodes), {call, ?MODULE, node_join, [Node, BootstrapNode]}),
		?LET(Node, oneof(Nodes), {call, ?MODULE, node_leave, [Node]})
	]).

precondition(
	#state{bootstrap_node = BootstrapNode},
	{call, ?MODULE, node_leave, [Node]}
) ->
	Node =/= BootstrapNode;
precondition(_, {call, ?MODULE, node_join, [Node, BootstrapNode]}) ->
	Node =/= BootstrapNode;
precondition(_, {call, _Mod, _Fun, _Args}) ->
	true.

postcondition(
	#state{nodes = Nodes},
	{call, ?MODULE, new_node, []},
	NewNode
) ->
	with_fake_timers(fun() ->
		#{overlay := NodeAddress} = horde:info(NewNode, address),
		?assertEqual(standalone, horde:info(NewNode, status)),
		lists:all(
			fun(Node) ->
				?WHEN(Node =/= NewNode andalso horde:info(Node, status) =:= ready,
					ok =:= ?assertEqual(error, horde:lookup(Node, NodeAddress, infinity)))
			end,
			Nodes
		)
	end);
postcondition(
	#state{nodes = Nodes},
	{call, ?MODULE, node_join, [JoinedNode, _]},
	Joined
) ->
	with_fake_timers(fun() ->
		#{transport := TransportAddress, overlay := OverlayAddress} =
			horde:info(JoinedNode, address),
		?assert(Joined),
		?assertEqual(ready, horde:info(JoinedNode, status)),
		lists:all(
			fun(Node) ->
				Queries = horde:info(Node, queries),
				?WHEN(horde:info(Node, status) =:= ready,
					ok =:= ?assertEqual(
						{Node, Queries, {ok, TransportAddress}},
						{Node, Queries, horde:lookup(Node, OverlayAddress, infinity)}
					))
			end,
			Nodes
		)
	end);
postcondition(
	#state{nodes = Nodes},
	{call, ?MODULE, node_leave, [LeftNode]},
	Address
) ->
	with_fake_timers(fun() ->
		lists:all(
			fun(Node) ->
				?WHEN(LeftNode =/= Node andalso (horde:info(Node, status) =:= ready),
					begin
						ok =:= ?assertEqual(pang, horde:ping(Node, {compound, Address}))
					end)
			end,
			Nodes
		)
	end);
postcondition(_State, {call, _Mod, _Fun, _Args}, _Res) ->
	true.

next_state(
	#state{} = State,
	_Node,
	{call, ?MODULE, node_join, [_, _]}
) ->
	State;
next_state(
	#state{bootstrap_node = BootstrapNode, nodes = Nodes} = State,
	Node,
	{call, ?MODULE, new_node, []}
) ->
	State#state{
		bootstrap_node = case BootstrapNode =:= undefined of
			true -> Node;
			false -> BootstrapNode
		end,
		nodes = ordsets:add_element(Node, Nodes)
	};
next_state(
	#state{nodes = Nodes} = State,
	_,
	{call, ?MODULE, node_leave, [Node]}
) ->
	State#state{nodes = ordsets:del_element(Node, Nodes)}.

% Helpers

new_node() -> horde_test_utils:create_node().

node_join(Node, BootstrapNode) ->
	with_fake_timers(fun() ->
		#{transport := TransportAddress} = horde:info(BootstrapNode, address),
		horde:join(Node, [{transport, TransportAddress}], infinity)
	end).

node_leave(Node) ->
	NodeAddress = horde:info(Node, address),
	horde:stop(Node),
	NodeAddress.

with_fake_timers(Fun) ->
	Policy = fake_time:combine_policies([
		{fun pass_one_check_ring/2, sets:new()},
		fun check_query_timeout/1
	]),
	fake_time:process_timers(Policy),
	fake_time:with_policy(fun check_query_timeout/1, Fun).

pass_one_check_ring({_, Node, {timeout, _, check_ring}}, CheckedNodes) ->
	case sets:is_element(Node, CheckedNodes) of
		true -> {delay, CheckedNodes};
		false -> {trigger, sets:add_element(Node, CheckedNodes)}
	end;
pass_one_check_ring(_, State) ->
	{delay, State}.

check_query_timeout({_, _, {timeout, _, {query_timeout, _}}} = Timer) ->
	timer:apply_after(0, ?MODULE, do_check_query_timeout, [Timer]),
	delay;
check_query_timeout(_) ->
	delay.

do_check_query_timeout({QueryTimer, Node, {timeout, _, {query_timeout, QueryRef}}}) ->
	case horde:query_info(Node, QueryRef) of
		#{destination := {compound, #{transport := {_, Pid}}}} ->
			case is_process_alive(Pid) of
				true ->
					ok;
				false ->
					fake_time:trigger_timer(QueryTimer)
			end;
		#{destination := {transport, {_, Pid}}} ->
			case is_process_alive(Pid) of
				true ->
					ok;
				false ->
					fake_time:trigger_timer(QueryTimer)
			end;
		undefined ->
			ok
	end.

format_state(State) ->
	maps:from_list(
		lists:zip(
			record_info(fields, state),
			tl(tuple_to_list(State))
		)
	).

format_history(History, Cmd) -> format_history(History, Cmd, #{}).

format_history([], [], _) ->
	ok;
format_history(_, [], _) ->
	ok;
format_history([], [Cmd | _], Vars) ->
	format_command(Cmd, crash, Vars);
format_history([{State, Result} | History], [Cmd | Cmds], Vars) ->
	io:format(user, "State: ~p~n", [format_state(State)]),
	Vars2 = format_command(Cmd, Result, Vars),
	io:format(user, "~n", []),
	format_history(History, Cmds, Vars2).

format_command({set, {var, Var}, {call, M, F, A}}, Result, Vars) ->
	RenderedArgs = lists:map(fun(Arg) -> replace_symbol(Arg, Vars) end, A),
	io:format(user,
		"Var~p = ~p:~p~p % ~p~n",
		[Var, M, F, RenderedArgs, Result]
	),
	Vars#{Var => Result};
format_command(Cmd, Result, _) ->
	io:format(user, "~p -> ~p~n", [Cmd, Result]).

replace_symbol({var, Var}, Vars) -> maps:get(Var, Vars);
replace_symbol(X, _Vars) -> X.
