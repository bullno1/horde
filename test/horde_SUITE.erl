-module(horde_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
	[{group, unstable},
	 no_self_join,
	 props].

groups() ->
	[{unstable, [{repeat_until_any_fail, 5}], [bootstrap]}].

init_per_suite(Config) ->
	{ok, Apps} = application:ensure_all_started(horde),
	[{apps, Apps} | Config].

end_per_suite(Config) ->
	Apps = ?config(apps, Config),
	_ = [application:stop(App) || App <- Apps],
	ok.

init_per_testcase(_Testcase, Config) ->
	fake_time:start_link(),
	horde_addr_gen:start_link([exs1024s, {123, 123534, 345345}]),
	Config.

end_per_testcase(_Testcase, _Config) ->
	horde_addr_gen:stop(),
	fake_time:stop(),
	ok.

no_self_join(_Config) ->
	Node = horde_test_utils:create_node(),
	#{transport := TransportAddress} = horde:info(Node, address),
	?assertEqual(false, horde:join(Node, [{transport, TransportAddress}])),
	horde:stop(Node).

props(_Config) ->
	?assertEqual([], proper:module(horde_prop, [10000, {to_file, user}])).

bootstrap() -> [{timetrap, 20000}].

bootstrap(_Config) ->
	NumNodes = 128,
	Nodes = lists:map(
		fun(_) ->
			horde_test_utils:create_node()
		end,
		lists:seq(1, NumNodes)
	),
	_ = [
		?assertEqual(standalone, horde:info(Node, status)) || Node <- Nodes
	],

	[BootstrapNode | _] = Nodes,
	#{transport := BootstrapAddress} = horde:info(BootstrapNode, address),
	BootstrapNodes = [{transport, BootstrapAddress}],
	Joins = [
		{Node, spawn_monitor(fun() -> horde:join(Node, BootstrapNodes) end)}
		|| Node <- tl(Nodes)
	],
	_ = [
		receive
			{'DOWN', MonitorRef, _, _, Reason} ->
				?assertEqual(normal, Reason),
				?assertEqual(joined, horde:info(Node, status))
		end
		|| {Node, {_, MonitorRef}} <- Joins
	],
	% A node's ring must not contains itself
	lists:foreach(
		fun(Node) ->
			Ring = horde:info(Node, ring),
			#{overlay := OverlayAddress} = horde:info(Node, address),
			?assertEqual(none, horde_ring:lookup(OverlayAddress, Ring))
		end,
		Nodes
	),
	_ = [?assertEqual(joined, horde:info(Node, status)) || Node <- Nodes],
	% All nodes must have a correct local view of the swarm
	MaxAddress = horde_crypto:info(horde_crypto:default(), max_address),
	FullRing =
		lists:foldl(
			fun(Node, Acc) ->
				CompoundAddress = #{overlay := OverlayAddress} = horde:info(Node, address),
				horde_ring:insert(OverlayAddress, CompoundAddress, Acc)
			end,
			horde_ring:new(fun(Addr) -> MaxAddress - Addr end),
			Nodes
		),
	lists:foreach(
		fun(Node) ->
			Successor = #{address := SuccessorAddress} = horde:info(Node, successor),
			Predecessor = #{address := PredecessorAddress} = horde:info(Node, predecessor),
			#{overlay := OverlayAddress} = horde:info(Node, address),
			?assertEqual({OverlayAddress, [SuccessorAddress]}, {OverlayAddress, horde_ring:successors(OverlayAddress, 1, FullRing)}),
			?assertEqual({OverlayAddress, [PredecessorAddress]}, {OverlayAddress, horde_ring:predecessors(OverlayAddress, 1, FullRing)}),

			% Neighbour pointers must be kept up-to-date
			NodeRing = horde:info(Node, ring),
			[RingSuccessor] = horde_ring:successors(OverlayAddress, 1, NodeRing),
			[RingPredecessor] = horde_ring:predecessors(OverlayAddress, 1, NodeRing),
			?assertEqual(maps:get(address, RingSuccessor), maps:get(address, Successor)),
			?assertEqual(maps:get(address, RingPredecessor), maps:get(address, Predecessor)),
			?assert(maps:get(last_seen, Successor) >= maps:get(last_seen, RingSuccessor)),
			?assert(maps:get(last_seen, Predecessor) >= maps:get(last_seen, RingPredecessor))
		end,
		Nodes
	),
	% Any node can lookup other nodes
	lists:foreach(
		fun(Node) ->
			lists:foreach(
				fun(OtherNode) ->
					#{overlay := OverlayAddress, transport := TransportAddress}
						= horde:info(OtherNode, address),
					ok =:= ?assertEqual(
						{ok, TransportAddress},
						horde_test_utils:lookup_traced(
							Node, OverlayAddress, false, true
						)
					)
				end,
				Nodes
			)
		end,
		Nodes
	),
	_ = [horde:stop(Node) || Node <- Nodes],
	ok.
