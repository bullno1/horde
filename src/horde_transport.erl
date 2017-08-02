-module(horde_transport).
-export_type([ref/0, ctx/0, address/0]).
-export([
	open/2,
	send/4,
	recv_async/1,
	close/1,
	notify/3,
	compound_address/2
]).

-opaque ref() :: {module(), term()}.
-opaque address() :: {module(), term()}.
-type ctx() :: #{
	module := module(),
	controlling_process := pid(),
	crypto := horde_crypto:ctx(),
	keypair := horde_crypto:keypair()
}.
-type opts() :: #{
	crypto := horde_crypto:ctx(),
	keypair := horde_crypto:keypair(),
	transport_opts := term()
}.
-type event()
	:: {message, horde:message_header(), horde:message_body()}
	 | {transport_error, Error :: term(), Id :: term()}.

% callback

-callback open(Ctx, Opts) -> {ok, Ref} | {error, Reason} when
	Ctx :: ctx(),
	Opts :: term(),
	Ref :: term(),
	Reason :: term().

-callback send(Ref, Address, Id, Body) -> any() when
	Ref :: term(),
	Address :: term(),
	Id :: term(),
	Body :: horde:message_body().

-callback recv_async(Ref) -> any() when
	Ref :: term().

-callback close(Ref) -> any() when
	Ref :: term().

% API

-spec open(module(), opts()) -> {ok, ref()} | {error, term()}.
open(
	Module,
	#{crypto := Crypto, keypair := Keypair, transport_opts := TransportOpts}
) ->
	Ctx = #{
		module => Module,
		controlling_process => self(),
		crypto => Crypto,
		keypair => Keypair
	},
	case Module:open(Ctx, TransportOpts) of
		{ok, Ref} -> {ok, {Module, Ref}};
		{error, _} = Err -> Err
	end.

-spec send(ref(), address(), term(), horde:message_body()) -> ok.
send({Module, Ref}, {Module, Address}, Id, Body) ->
	Module:send(Ref, Address, Id, Body),
	ok.

-spec recv_async(ref()) -> ok.
recv_async({Module, Ref}) -> _ = Module:recv_async(Ref), ok.

-spec close(ref()) -> ok.
close({Module, Ref}) -> _ = Module:close(Ref), ok.

-spec notify(ctx(), term(), event()) -> ok.
notify(
	#{module := Module, controlling_process := ControllingProcess}, Ref, Event
) ->
	ControllingProcess ! {?MODULE, {Module, Ref}, Event},
	ok.

-spec compound_address(ctx(), term()) -> horde:compound_address().
compound_address(
	#{crypto := Crypto, keypair := {PubKey, _}, module := Module}, TransportAddress
) ->
	#{
		overlay => horde_crypto:address_of(Crypto, PubKey),
		transport => {Module, TransportAddress}
	}.
