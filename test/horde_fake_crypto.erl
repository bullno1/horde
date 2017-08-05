-module(horde_fake_crypto).
-behaviour(horde_crypto).
% API
-export([reset_seed/1]).
% horde_crypto
-export([
	init/1,
	info/2,
	generate_keypair/1,
	address_of/2,
	sign/3,
	verify/4,
	serialize/3,
	deserialize/3
]).

% API

reset_seed(Seed) -> gen_server:call(horde_fake_crypto_server, {reset_seed, Seed}).

% horde_crypto

init(_) -> [].

info(_, max_address) -> gen_server:call(horde_fake_crypto_server, max_address).

generate_keypair(_) -> gen_server:call(horde_fake_crypto_server, generate_keypair).

address_of(_, Key) -> Key.

sign(_, _Message, _) -> <<>>.

verify(_, _, _, _) -> true.

serialize(_, _, Obj) -> Obj.

deserialize(_, _, Obj) -> Obj.
