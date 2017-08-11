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
			Parent = self(),
			Pid = spawn(fun() ->
				horde_addr_gen:reset([exs1024s, {S1, S2, S3}]),
				{History, State, Result} = run_commands(?MODULE, Commands),
				_ = [catch horde:stop(Node) || Node <- State#state.nodes],
				Parent ! {self(), History, State, Result}
			end),

			MonitorRef = monitor(process, Pid),
			Stacktrace =
				receive
					{'DOWN', MonitorRef, process, Pid, _} ->
						[]
				after 5000 ->
					CurrentStacktrace =
						try erlang:process_info(Pid, current_stacktrace) of
							{current_stacktrace, ST} -> ST;
							_ -> []
						catch
							error:badarg -> []
						end,
					exit(Pid, kill),
					CurrentStacktrace
				end,

			receive
				{Pid, History, State, Result} ->
					?WHENFAIL(
						io:format(user, "~nCommands:~n~s~nHistory: ~p~nState: ~p~nResult: ~p~n", [
							pretty_print(Commands), History, State, Result
						]),
						aggregate(command_names(Commands), Result =:= ok)
					)
			after 0 ->
				io:format(user, "~nCommands:~n~s~nReason: ~p~nStacktrace: ~p~n", [
					pretty_print(Commands), timeout, Stacktrace
				]),
				false
			end
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
				?WHEN(Node =/= NewNode andalso horde:info(Node, status) =:= joined,
					ok =:= ?assertEqual(error, horde_test_utils:lookup_traced(Node, NodeAddress, true, false)))
			end,
			Nodes
		)
	end);
postcondition(
	#state{nodes = Nodes},
	{call, ?MODULE, node_join, [JoinedNode, _]},
	Joined
) ->
	?assert(Joined),
	with_fake_timers(fun() ->
		#{transport := TransportAddress, overlay := OverlayAddress} =
			CompoundAddress = horde:info(JoinedNode, address),
		lists:foreach(fun wait_until_stable/1, Nodes),
		?assertEqual({OverlayAddress, joined}, {OverlayAddress, horde:info(JoinedNode, status)}),
		lists:all(
			fun(Node) ->
				Queries = horde:info(Node, queries),
				?WHEN(horde:info(Node, status) =:= joined,
					ok =:= ?assertEqual(
						{Node, Queries, {ok, TransportAddress}},
						{Node, Queries, horde_test_utils:lookup_traced(Node, OverlayAddress, false, true)}
					)
					andalso
					ok =:= ?assertEqual(
						pong, horde:ping(Node, {compound, CompoundAddress})
					)
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
		% Only ping with bootstrap node to leave the stale entry in other node's
		% caches
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
		horde:join(Node, [{transport, TransportAddress}])
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
	fake_time:with_policy(Policy, Fun).

pass_one_check_ring({_, Node, {timeout, _, check_ring}}, CheckedNodes) ->
	case sets:is_element(Node, CheckedNodes) of
		true -> {delay, CheckedNodes};
		false -> {trigger, sets:add_element(Node, CheckedNodes)}
	end;
pass_one_check_ring(_, State) ->
	{delay, State}.

check_query_timeout({TimerRef, Node, {timeout, _, {query_timeout, QueryRef}}}) ->
	QueriedProcess =
		case horde:query_info(Node, QueryRef) of
			#{destination := {compound, #{transport := {_, Pid}}}} ->
				Pid;
			#{destination := {transport, {_, Pid}}} ->
				Pid;
			undefined ->
				undefined
		end,
	case is_pid(QueriedProcess) andalso is_process_alive(QueriedProcess) of
		true ->
			on_down(QueriedProcess,
				fun() ->
					fake_time:trigger_timer(TimerRef)
				end),
			delay;
		false ->
			trigger
	end;
check_query_timeout(_) ->
	delay.

on_down(Pid, Fun) ->
	spawn(fun() ->
		MonitorRef = monitor(process, Pid),
		receive
			{'DOWN', MonitorRef, process, Pid, _} -> Fun()
		end
	end).

wait_until_stable(Node) ->
	horde:wait(Node, fun(_) ->
		case maps:size(horde:info(Node, queries)) =:= 0 of
			true -> {stop, ok};
			false -> wait
		end
	end).
