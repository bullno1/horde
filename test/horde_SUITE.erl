-module(horde_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").

all() -> [bootstrap].

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

bootstrap(_Config) ->
	meck:expect(horde_mock, start_timer,
		fun(_Timeout, Dest, Message) ->
			erlang:start_timer(0, Dest, Message)
		end
	),
	meck:expect(horde_mock, cancel_timer, fun erlang:cancel_timer/1),

	{ok, BootstrapNode} = create_node(),
	Transport = horde:info(BootstrapNode, transport),
	BootstrapAddress = horde_transport:info(Transport, address),
	NumNodes = 16,
	BootstrapNodes = [{transport, BootstrapAddress}],
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
	true = lists:all(
		fun(Node) -> horde:join(Node, BootstrapNodes, infinity) end,
		Nodes
	),
	true = lists:all(
		fun(Node) -> ready =:= horde:info(Node, status) end,
		Nodes
	),
	_ = [horde:stop(Node) || Node <- Nodes],
	horde:stop(BootstrapNode),
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
