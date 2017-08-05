-module(horde_test_utils).
-export([create_node/0, create_node/1]).

create_node() ->
	create_node(#{
		address => horde_addr_gen:next_address(),
		max_address => horde_addr_gen:max_address()
	}).

create_node(Opts) ->
	DefaultOpts = #{
		transport => {horde_disterl, #{active => false}}
	},
	{ok, Node} = horde:start_link(maps:merge(DefaultOpts, Opts)),
	Node.
