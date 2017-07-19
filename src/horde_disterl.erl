-module(horde_disterl).
-behaviour(horde_transport).
-export([
	init/1,
	info/2,
	send/3,
	recv_async/1,
	terminate/1
]).

% horde_transport

init(_Opts) -> {ok, []}.

info(_Ref, address) -> self().

send(_Ref, Address, Message) -> Address ! {horde_transport, Message}.

recv_async(_Ref) -> ok.

terminate(_) -> ok.
