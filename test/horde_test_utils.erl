-module(horde_test_utils).
-export([create_node/0, create_node/1]).

create_node() -> create_node(#{}).

create_node(Opts) ->
	Crypto = horde_crypto:init(horde_fake_crypto, []),
	DefaultOpts = #{
		crypto => Crypto,
		keypair => horde_crypto:generate_keypair(Crypto),
		transport => {horde_disterl, #{active => false}}
	},
	{ok, Node} = horde:start_link(maps:merge(DefaultOpts, Opts)),
	Node.
