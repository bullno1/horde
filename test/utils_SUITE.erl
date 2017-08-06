-module(utils_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [horde_address_spec, crypto, address].

init_per_suite(Config) ->
	{ok, Apps} = application:ensure_all_started(horde),
	[{apps, Apps} | Config].

end_per_suite(Config) ->
	Apps = ?config(apps, Config),
	_ = [application:stop(App) || App <- Apps],
	ok.

crypto(_Config) ->
	Crypto = horde_crypto:default(),
	{PK, SK} = horde_crypto:generate_keypair(Crypto),

	Address = horde_crypto:address_of(Crypto, PK),
	MaxAddress = horde_crypto:info(Crypto, max_address),
	?assertEqual(true, Address =< MaxAddress),

	{PK2, _SK2} = horde_crypto:generate_keypair(Crypto),
	Address2 = horde_crypto:address_of(Crypto, PK2),
	?assertEqual(true, Address =/= Address2),
	?assertEqual(true, Address2 =< MaxAddress),

	Message = crypto:strong_rand_bytes(16),
	Sig = horde_crypto:sign(Crypto, Message, SK),
	?assertEqual(true, horde_crypto:verify(Crypto, Message, Sig, PK)),

	Message2 = crypto:strong_rand_bytes(8),
	?assertEqual(false, horde_crypto:verify(Crypto, Message2, Sig, PK)),

	RandomSig = crypto:strong_rand_bytes(8),
	?assertEqual(false, horde_crypto:verify(Crypto, Message, RandomSig, PK)),

	RandomSig2 = crypto:strong_rand_bytes(256),
	?assertEqual(false, horde_crypto:verify(Crypto, Message, RandomSig2, PK)),

	PK = horde_crypto:deserialize(Crypto, key, horde_crypto:serialize(Crypto, key, PK)),

	ok.

address(_Config) ->
	?assertEqual(true, horde_utils:is_address_between(6, 5, 10)),
	?assertEqual(false, horde_utils:is_address_between(5, 5, 10)),
	?assertEqual(false, horde_utils:is_address_between(4, 5, 10)),
	?assertEqual(false, horde_utils:is_address_between(10, 5, 10)),
	?assertEqual(false, horde_utils:is_address_between(11, 5, 10)),

	?assertEqual(true, horde_utils:is_address_between(11, 10, 5)),
	?assertEqual(false, horde_utils:is_address_between(10, 10, 5)),
	?assertEqual(false, horde_utils:is_address_between(9, 10, 5)),
	?assertEqual(false, horde_utils:is_address_between(6, 10, 5)),

	LTE = fun erlang:'=<'/2,
	?assertEqual(true, horde_utils:is_address_between(5, 5, 10, LTE, LTE)),

	ok.

horde_address_spec(_Config) ->
	?assert(proper:check_spec({horde_utils, is_address_between, 3}, [{to_file, user}])),
	?assert(proper:check_spec({horde_utils, is_address_between, 5}, [{to_file, user}])).
