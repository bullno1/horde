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
	meck:new([horde_mock], [non_strict]),
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
	Transport = horde:info(Node, transport),
	TransportAddress = horde_transport:info(Transport, address),
	false = horde:join(Node, [{transport, TransportAddress}], infinity),
	horde:stop(Node).

bootstrap() -> [{timetrap, 3000}].

bootstrap(_Config) ->
	meck:expect(horde_mock, start_timer,
		fun(_Timeout, Dest, Message) ->
			erlang:start_timer(0, Dest, Message)
		end
	),
	meck:expect(horde_mock, cancel_timer, fun erlang:cancel_timer/1),

	NumNodes = 64,
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
	Transport = horde:info(BootstrapNode, transport),
	BootstrapAddress = horde_transport:info(Transport, address),
	BootstrapNodes = [{transport, BootstrapAddress}],
	%horde_disterl:set_message_allowed(false),
	true = lists:all(
		fun(Node) ->
			%horde_disterl:set_message_allowed(true),
			Joined = horde:join(Node, BootstrapNodes, infinity),
			%horde_disterl:set_message_allowed(false),
			Joined
		end,
		tl(Nodes)
	),
	% A node's ring must not contains itself
	true = lists:all(
		fun(Node) ->
			Ring = horde:info(Node, ring),
			OverlayAddress = horde:info(Node, address),
			horde_ring:lookup(OverlayAddress, Ring) =:= none
		end,
		Nodes
	),
	true = lists:all(
		fun(Node) -> ready =:= horde:info(Node, status) end,
		Nodes
	),
	% Any node can lookup other nodes
	lists:all(
		fun(Node) ->
			lists:all(
				fun(OtherNode) ->
					OverlayAddress = horde:info(OtherNode, address),
					NodeTransport = horde:info(OtherNode, transport),
					TransportAddress = horde_transport:info(NodeTransport, address),
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
	Crypto = horde_crypto:default(),
	DefaultOpts = #{
		crypto => Crypto,
		keypair => horde_crypto:generate_keypair(Crypto),
		transport => {horde_disterl, #{active => false}}
	},
	horde:start_link(maps:merge(DefaultOpts, Opts)).
