-module(horde_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
	[{group, unstable},
	 no_self_join,
	 props].

groups() ->
	[{unstable, [{repeat_until_any_fail, 50}], [bootstrap]}].

init_per_suite(Config) ->
	{ok, Apps} = application:ensure_all_started(horde),
	[{apps, Apps} | Config].

end_per_suite(Config) ->
	Apps = ?config(apps, Config),
	_ = [application:stop(App) || App <- Apps],
	ok.

init_per_testcase(_Testcase, Config) ->
	fake_time:start_link(),
	horde_fake_crypto_server:start_link([exs1024s, {123, 123534, 345345}]),
	Config.

end_per_testcase(_Testcase, _Config) ->
	horde_fake_crypto_server:stop(),
	fake_time:stop(),
	ok.

no_self_join(_Config) ->
	Node = horde_test_utils:create_node(),
	#{transport := TransportAddress} = horde:info(Node, address),
	?assertEqual(false, horde:join(Node, [{transport, TransportAddress}], infinity)),
	horde:stop(Node).

props(_Config) ->
	?assertEqual([], proper:module(horde_prop, [10000, {to_file, user}])).

bootstrap() -> [{timetrap, 10000}].

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
	lists:foreach(
		fun(Node) -> horde:join_async(Node, BootstrapNodes) end,
		tl(Nodes)
	),
	_ = [
		?assertEqual(true, horde:wait_join(Node, infinity))
		|| Node <- tl(Nodes)
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
	_ = [?assertEqual(ready, horde:info(Node, status)) || Node <- Nodes],
	% All nodes must have a correct local view of the swarm
	MaxAddress = horde_crypto:info(horde_crypto:default(), max_address),
	FullRing =
		lists:foldl(
			fun(Node, Acc) ->
				#{overlay := OverlayAddress} = horde:info(Node, address),
				horde_ring:insert(OverlayAddress, Node, Acc)
			end,
			horde_ring:new(fun(Addr) -> MaxAddress - Addr end),
			Nodes
		),
	lists:foreach(
		fun(Node) ->
			Successor = #{address := SuccessorAddress} = horde:info(Node, successor),
			Predecessor = #{address := PredecessorAddress} = horde:info(Node, predecessor),
			#{overlay := OverlayAddress} = horde:info(Node, address),
			[SuccessorNode] = horde_ring:successors(OverlayAddress, 1, FullRing),
			[PredecessorNode] = horde_ring:predecessors(OverlayAddress, 1, FullRing),
			?assertEqual(SuccessorAddress, horde:info(SuccessorNode, address)),
			?assertEqual(PredecessorAddress, horde:info(PredecessorNode, address)),

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
	lists:all(
		fun(Node) ->
			lists:all(
				fun(OtherNode) ->
					#{overlay := OverlayAddress, transport := TransportAddress}
						= horde:info(OtherNode, address),
					ok =:= ?assertEqual(
						{ok, TransportAddress},
						horde:lookup(Node, OverlayAddress, infinity)
					)
				end,
				Nodes
			)
		end,
		Nodes
	),
	_ = [horde:stop(Node) || Node <- Nodes],
	ok.
