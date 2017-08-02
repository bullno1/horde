-module(horde_transport).
-export_type([ref/0, address/0]).
-export([
	open/2,
	info/2,
	send/4,
	recv_async/1,
	close/1
]).

-opaque ref() :: {module(), term()}.
-opaque address() :: {module(), term()}.

% callback

-callback init(Opts) -> {ok, Ref} | {error, Reason} when
	Opts :: term(),
	Ref :: term(),
	Reason :: term().

-callback info(Ref, address) -> Address when
	Ref :: term(),
	Address :: term().

-callback send(Ref, Address, Id, Body) -> any() when
	Ref :: term(),
	Address :: term(),
	Id :: term(),
	Body :: horde:message_body().

-callback recv_async(Ref) -> any() when
	Ref :: term().

-callback terminate(Ref) -> any() when
	Ref :: term().

% API

-spec open(module(), term()) -> {ok, ref()} | {error, term()}.
open(Module, Opts) ->
	case Module:init(Opts) of
		{ok, Ref} -> {ok, {Module, Ref}};
		{error, _} = Err -> Err
	end.

-spec info(ref(), address) -> address().
info({Module, Ref}, Info) -> {Module, Module:info(Ref, Info)}.

-spec send(ref(), address(), term(), horde:message_body()) -> ok.
send({Module, Ref}, {Module, Address}, Id, Body) ->
	Module:send(Ref, Address, Id, Body),
	ok.

-spec recv_async(ref()) -> ok.
recv_async({Module, Ref}) -> _ = Module:recv_async(Ref), ok.

-spec close(ref()) -> ok.
close({Module, Ref}) -> _ = Module:terminate(Ref), ok.
