-module(horde_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").

all() -> [bootstrap, no_self_join].

init_per_suite(Config) ->
	{ok, Apps} = application:ensure_all_started(horde),
	[{apps, Apps} | Config].

end_per_suite(Config) ->
	Apps = ?config(apps, Config),
	_ = [application:stop(App) || App <- Apps],
	ok.

init_per_testcase(_Testcase, Config) ->
	meck:new([horde_mock], [non_strict, no_history]),
	Config.

end_per_testcase(_Testcase, _Config) ->
	meck:unload([horde_mock]),
	ok.

no_self_join(_Config) ->
	meck:expect(horde_mock, start_timer,
		fun(_Timeout, Dest, Message) ->
			erlang:start_timer(0, Dest, Message)
		end
	),
	meck:expect(horde_mock, cancel_timer, fun erlang:cancel_timer/1),

	{ok, Node} = create_node(),
	#{transport := TransportAddress} = horde:info(Node, address),
	false = horde:join(Node, [{transport, TransportAddress}], infinity),
	horde:stop(Node).

bootstrap() -> [{timetrap, 5000}].

bootstrap(_Config) ->
	meck:expect(horde_mock, start_timer,
		fun(_Timeout, _Dest, _Message) -> make_ref() end
	),
	meck:expect(horde_mock, cancel_timer, fun(_) -> ok end),

	NumNodes = 128,
	Nodes = lists:map(
		fun(_) ->
			{ok, Pid} = create_node(),
			Pid
		end,
		lists:seq(1, NumNodes)
	),
	true = lists:all(
		fun(Node) -> standalone =:= horde:info(Node, status) end,
		Nodes
	),

	[BootstrapNode | _] = Nodes,
	#{transport := BootstrapAddress} = horde:info(BootstrapNode, address),
	BootstrapNodes = [{transport, BootstrapAddress}],
	lists:foreach(
		fun(Node) -> horde:join_async(Node, BootstrapNodes) end,
		tl(Nodes)
	),
	true = lists:all(
		fun(Node) -> horde:wait_join(Node, infinity) end,
		tl(Nodes)
	),
	% A node's ring must not contains itself
	true = lists:all(
		fun(Node) ->
			Ring = horde:info(Node, ring),
			#{overlay := OverlayAddress} = horde:info(Node, address),
			horde_ring:lookup(OverlayAddress, Ring) =:= none
		end,
		Nodes
	),
	true = lists:all(
		fun(Node) -> ready =:= horde:info(Node, status) end,
		Nodes
	),
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
			#{address := SuccessorAddress} = horde:info(Node, successor),
			#{address := PredecessorAddress} = horde:info(Node, predecessor),
			#{overlay := OverlayAddress} = horde:info(Node, address),
			[SuccessorNode] = horde_ring:successors(OverlayAddress, 1, FullRing),
			[PredecessorNode] = horde_ring:predecessors(OverlayAddress, 1, FullRing),
			SuccessorAddress = horde:info(SuccessorNode, address),
			PredecessorAddress = horde:info(PredecessorNode, address)
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
					horde:lookup(Node, OverlayAddress, infinity) =:= {ok, TransportAddress}
				end,
				Nodes
			)
		end,
		Nodes
	),
	_ = [horde:stop(Node) || Node <- Nodes],
	ok.

create_node() -> create_node(#{}).

create_node(Opts) ->
	CryptoMod = horde_ecdsa,
	CryptoOpts = #{
		hash_algo => sha256,
		curve => secp384r1,
		address_size => 160
	},
	Crypto = horde_crypto:init(CryptoMod, CryptoOpts),
	DefaultOpts = #{
		crypto => Crypto,
		keypair => horde_crypto:generate_keypair(Crypto),
		transport => {horde_disterl, #{active => true}}
	},
	horde:start_link(maps:merge(DefaultOpts, Opts)).
