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
	?FORALL(
		{Commands, S1, S2, S3},
		{commands(?MODULE),
		 non_neg_integer(), non_neg_integer(), non_neg_integer()},
		begin
			horde_addr_gen:reset_seed([exs1024s, {S1, S2, S3}]),
			{History, State, Result} = run_commands(?MODULE, Commands),
			_ = [catch horde:stop(Node) || Node <- State#state.nodes],

			?WHENFAIL(
				io:format(user, "Commands:~n~s~nHistory: ~p~nState: ~p~nResult: ~p~n", [
					pretty_print(Commands), History, State, Result
				]),
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
					ok =:= ?assertEqual(error, horde:lookup(Node, NodeAddress, 5000)))
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
			CompoundAddress = horde:info(JoinedNode, address),
		?assert(Joined),
		?assertEqual(ready, horde:info(JoinedNode, status)),
		lists:all(
			fun(Node) ->
				Queries = horde:info(Node, queries),
				?WHEN(horde:info(Node, status) =:= ready,
					ok =:= ?assertEqual(
						{Node, Queries, {ok, TransportAddress}},
						{Node, Queries, horde:lookup(Node, OverlayAddress, 5000)}
					))
					andalso
					ok =:= ?assertEqual(
						pong, horde:ping(Node, {compound, CompoundAddress})
					)
			end,
			Nodes
		)
	end);
postcondition(
	#state{bootstrap_node = BootstrapNode},
	{call, ?MODULE, node_leave, [_]},
	Address
) ->
	with_fake_timers(fun() ->
		ok =:= ?assertEqual(pang, horde:ping(BootstrapNode, {compound, Address}))
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
		horde:join(Node, [{transport, TransportAddress}], 5000)
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
