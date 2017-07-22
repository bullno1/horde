-module(utils_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").

all() -> [crypto, address].

init_per_suite(Config) ->
	{ok, Apps} = application:ensure_all_started(horde),
	[{apps, Apps} | Config].

end_per_suite(Config) ->
	Apps = ?config(apps, Config),
	_ = [application:stop(App) || App <- Apps],
	ok.

crypto(_Config) ->
	#{crypto := {CryptoMod, CryptoOpts}} = Realm = horde:default_realm(),
	{PK, SK} = horde:generate_keypair(Realm),
	Crypto = horde_crypto:init(CryptoMod, CryptoOpts),

	Address = horde_crypto:address_of(Crypto, PK),
	MaxAddress = horde_crypto:info(Crypto, max_address),
	true = Address =< MaxAddress,

	{PK2, _SK2} = horde:generate_keypair(Realm),
	Address2 = horde_crypto:address_of(Crypto, PK2),
	true = Address =/= Address2,
	true = Address2 =< MaxAddress,

	Message = crypto:strong_rand_bytes(16),
	Sig = horde_crypto:sign(Crypto, Message, SK),
	true = horde_crypto:verify(Crypto, Message, Sig, PK),

	Message2 = crypto:strong_rand_bytes(8),
	false = horde_crypto:verify(Crypto, Message2, Sig, PK),

	RandomSig = crypto:strong_rand_bytes(8),
	false = horde_crypto:verify(Crypto, Message, RandomSig, PK),

	RandomSig2 = crypto:strong_rand_bytes(256),
	false = horde_crypto:verify(Crypto, Message, RandomSig2, PK),

	PK = horde_crypto:deserialize(Crypto, key, horde_crypto:serialize(Crypto, key, PK)),

	ok.

address(_Config) ->
	true = horde_address:is_between(6, 5, 10),
	false = horde_address:is_between(5, 5, 10),
	false = horde_address:is_between(4, 5, 10),
	false = horde_address:is_between(10, 5, 10),
	false = horde_address:is_between(11, 5, 10),

	true = horde_address:is_between(11, 10, 5),
	false = horde_address:is_between(10, 10, 5),
	false = horde_address:is_between(9, 10, 5),
	false = horde_address:is_between(6, 10, 5),

	LTE = fun erlang:'=<'/2,
	true = horde_address:is_between(5, 5, 10, LTE, LTE),

	ok.
