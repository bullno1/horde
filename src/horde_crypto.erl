-module(horde_crypto).
-export_type([ctx/0, keypair/0]).
-export([
	default/0,
	init/2,
	info/2,
	generate_keypair/1,
	address_of/2,
	sign/3,
	verify/4,
	serialize/3,
	deserialize/3
]).

-opaque ctx() :: {module(), term()}.
-type keypair() :: {PubKey :: term(), PrivKey :: term()}.

-callback init(Opts) -> Ctx when
	Opts :: term(),
	Ctx :: term().

-callback info(Ctx, max_address) -> Address when
	Ctx :: term(),
	Address :: horde:overlay_address().

-callback generate_keypair(Ctx) -> KeyPair when
	Ctx :: term(),
	KeyPair :: term().

-callback address_of(Ctx, PubKey) -> Address when
	Ctx :: term(),
	PubKey :: term(),
	Address :: horde:overlay_address().

-callback sign(Ctx, Message, PrivKey) -> Signature when
	Ctx :: term(),
	Message :: binary(),
	PrivKey :: term(),
	Signature :: binary().

-callback verify(Ctx, Message, Signature, PubKey) -> boolean() when
	Ctx :: term(),
	Message :: binary(),
	Signature :: binary(),
	PubKey :: term().

-callback serialize(Ctx, Type, Obj) -> Bin when
	Ctx :: term(),
	Type :: key,
	Obj :: term(),
	Bin :: binary().

-callback deserialize(Ctx, Type, Bin) -> {ok, Obj} | {error, Reason} when
	Ctx :: term(),
	Type :: key,
	Obj :: term(),
	Reason :: term(),
	Bin :: binary().

-spec default() -> {module(), term()}.
default() ->
	{horde_ecdsa, #{
		hash_algo => sha256,
		curve => secp384r1,
		address_size => 160
	}}.

-spec init(module(), term()) -> ctx().
init(Module, Opts) -> {Module, Module:init(Opts)}.

-spec info(ctx(), max_address) -> horde:overlay_address().
info({Module, Ctx}, Info) -> Module:info(Ctx, Info).

-spec generate_keypair(ctx()) -> keypair().
generate_keypair({Module, Ctx}) -> Module:generate_keypair(Ctx).

-spec address_of(ctx(), term()) -> horde:overlay_address().
address_of({Module, Ctx}, PubKey) -> Module:address_of(Ctx, PubKey).

-spec sign(ctx(), binary(), term()) -> binary().
sign({Module, Ctx}, Message, PrivKey) -> Module:sign(Ctx, Message, PrivKey).

-spec verify(ctx(), binary(), binary(), term()) -> boolean().
verify({Module, Ctx}, Message, Signature, PubKey) ->
	Module:verify(Ctx, Message, Signature, PubKey).

-spec serialize(ctx(), key, term()) -> binary().
serialize({Module, Ctx}, Type, Obj) -> Module:serialize(Ctx, Type, Obj).

-spec deserialize(ctx(), key, binary()) -> {ok, term()} | {error, term()}.
deserialize({Module, Ctx}, Type, Bin) -> Module:deserialize(Ctx, Type, Bin).
