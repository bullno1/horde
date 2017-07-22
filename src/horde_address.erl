-module(horde_address).
-export([
	is_between/3,
	is_between/5
]).

-type order_fun(T) :: fun((T, T) -> boolean()).

-spec is_between(T, T, T) -> boolean().
is_between(Address, Lowerbound, Upperbound) ->
	LessThan = fun erlang:'<'/2,
	is_between(Address, Lowerbound, Upperbound, LessThan, LessThan).

-spec is_between(T, T, T, order_fun(T), order_fun(T)) -> boolean().
is_between(Address, Lowerbound, Upperbound, LowerboundCheck, UpperboundCheck) ->
	case Lowerbound < Upperbound of
		true ->
			LowerboundCheck(Lowerbound, Address)
			andalso UpperboundCheck(Address, Upperbound);
		false ->
			LowerboundCheck(Lowerbound, Address)
			orelse UpperboundCheck(Address, Upperbound)
	end.
