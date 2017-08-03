-module(horde_utils).
-export([
	maybe_reply/2,
	is_address_between/3,
	is_address_between/5
]).

-type order_fun(T) :: fun((T, T) -> boolean()).

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

-spec maybe_reply(none | pid(), term()) -> ok.
maybe_reply(none, _Msg) -> ok;
maybe_reply(From, Msg) -> gen_server:reply(From, Msg).
