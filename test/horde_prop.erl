-module(horde_prop).
-compile(export_all).
-include_lib("proper/include/proper.hrl").
-record(state, {
	bootstrap_node,
	nodes = []
}).

% Property

prop_horde() ->
	?TIMEOUT(60000,
		?FORALL(Commands, commands(?MODULE),
			begin
				{History, State, Result} = run_commands(?MODULE, Commands),
				lists:foreach(fun horde:stop/1, State#state.nodes),

				?WHENFAIL(
					begin
						format_history(History, Commands),
						io:format(user, "Last state: ~p~n", [format_state(State)]),
						io:format(user, "Timers: ~p~n", [fake_time:get_timers()]),
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
		?LET(Node, oneof(Nodes),
			oneof([
				{call, ?MODULE, node_join, [Node, BootstrapNode]},
				{call, ?MODULE, node_leave, [Node]}
			]))
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
		horde:info(NewNode, status) =:= standalone
		andalso lists:all(
			fun(Node) ->
				not (Node =/= NewNode andalso horde:info(Node, status) =:= ready)
				orelse
				error =:= horde:lookup(Node, NodeAddress, 300)
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
		Joined
		andalso horde:info(JoinedNode, status) =:= ready
		andalso lists:all(
			fun(Node) ->
				not (horde:info(Node, status) =:= ready)
				orelse
				{ok, TransportAddress} =:= horde:lookup(Node, OverlayAddress, 300)
			end,
			Nodes
		)
	end);
postcondition(_State, {call, _Mod, _Fun, _Args}, _Res) ->
	true.

next_state(
	#state{nodes = Nodes} = State,
	_,
	{call, ?MODULE, node_leave, [Node]}
) ->
	State#state{nodes = lists:delete(Node, Nodes)};
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

with_fake_timers(Fun) ->
	fake_time:process_timers({fun timer_policy/2, sets:new()}),
	fake_time:with_policy({fun timer_policy/2, sets:new()}, Fun).

timer_policy({_, Node, {timeout, _, check_ring}}, CheckedNodes) ->
	case sets:is_element(Node, CheckedNodes) of
		true -> {delay, CheckedNodes};
		false -> {trigger, sets:add_element(Node, CheckedNodes)}
	end;
timer_policy({_, _, {timeout, _, {query_timeout, _}}} = Timer, State) ->
	timer:apply_after(0, ?MODULE, check_query_timeout, [Timer]),
	{delay, State}.

check_query_timeout({QueryTimer, Node, {timeout, _, {query_timeout, QueryRef}}}) ->
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
			delay
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
	with_fake_timers(fun() ->
		#{transport := TransportAddress} = horde:info(BootstrapNode, address),
		horde:join(Node, [{transport, TransportAddress}], 300)
	end).

node_leave(Node) ->
	horde:stop(Node).
