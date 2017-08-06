-module(horde_utils).
-export([
	maybe_reply/3,
	record_to_map/2,
	is_address_between/3,
	is_address_between/5
]).
-export_type([order_fun/1, reply_to/0]).

-type order_fun(T) :: fun((T, T) -> boolean()).
-type reply_to() :: none | {sync | async, {pid(), any()}}.

-spec is_address_between(T, T, T) -> boolean().
is_address_between(Address, Lowerbound, Upperbound) ->
	LessThan = fun erlang:'<'/2,
	is_address_between(Address, Lowerbound, Upperbound, LessThan, LessThan).

-spec is_address_between(T, T, T, order_fun(T), order_fun(T)) -> boolean().
is_address_between(
	Address, Lowerbound, Upperbound, LowerboundCheck, UpperboundCheck
) ->
	case Lowerbound < Upperbound of
		true ->
			LowerboundCheck(Lowerbound, Address)
			andalso UpperboundCheck(Address, Upperbound);
		false ->
			LowerboundCheck(Lowerbound, Address)
			orelse UpperboundCheck(Address, Upperbound)
	end.

-spec maybe_reply(module(), reply_to(), term()) -> ok.
maybe_reply(_Module, none, _Msg) ->
	ok;
maybe_reply(Module, {async, {Pid, Tag}}, Msg) ->
	Pid ! {Module, Tag, Msg},
	ok;
maybe_reply(_Module, {sync, Client}, Msg) ->
	gen_server:reply(Client, Msg),
	ok.

-spec record_to_map([atom()], tuple()) -> map().
record_to_map(Fields, Record) ->
	maps:from_list(lists:zip(Fields, tl(tuple_to_list(Record)))).
