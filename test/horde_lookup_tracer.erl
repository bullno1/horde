-module(horde_lookup_tracer).
-behaviour(horde_tracer).
-export([new/1]).
-export([init/1, handle_event/2]).

-record(state, {
	history = [],
	report_on_success,
	report_on_failure
}).

% API

new(Opts) -> horde_tracer:new(?MODULE, Opts).

% horde_tracer

init(#{
	report_on_success := ReportOnSucess,
	report_on_failure := ReportOnFailure
}) ->
	#state{
		report_on_success = ReportOnSucess,
		report_on_failure = ReportOnFailure
	}.

handle_event(
	{finish, {ok, _}} = Event,
	#state{report_on_success = true, history = History}
) ->
	report_history(lists:reverse([Event | History]));
handle_event(
	{finish, error} = Event,
	#state{report_on_failure = true, history = History}
) ->
	report_history(lists:reverse([Event | History]));
handle_event({finish, _}, State) ->
	State;
handle_event(Event, #state{history = History} = State) ->
	State#state{history = [Event | History]}.

% Private

report_history(History) ->
	ct:pal("Distances: ~w~nHistory: ~n~s", [
		[Distance || {query, #{distance := Distance}} <- History],
		format_history(History)
	]).

format_history(History) -> format_history(History, #{}, []).

format_history([], _Mappings, Acc) ->
	lists:reverse(Acc);
format_history([Event | Rest], Mappings, Acc) ->
	{NewLine, NewMappings} = case Event of
		{start, #{
			address := Address,
			max_address := MaxAddress,
			num_parallel_queries := NumParallelQueries}
		} ->
			Line = io_lib:format(
				"Target: ~w~nMax address: ~w~nNum parallel queries: ~w~n",
				[Address, MaxAddress, NumParallelQueries]
			),
			{Line, Mappings};
		{finish, Result} ->
			{io_lib:format("Result: ~p~n", [Result]), Mappings};
		{query, #{reference := Ref, endpoint := Endpoint, distance := Distance}} ->
			QueryNo = maps:size(Mappings) + 1,
			Line = io_lib:format(
				"send #~p (distance: ~p) -> ~p~n",
				[QueryNo, Distance, Endpoint]
			),
			{Line, Mappings#{Ref => QueryNo}};
		{reply, #{reference := Ref, result := Peers}} ->
			QueryNo = maps:get(Ref, Mappings),
			Line = io_lib:format(
				"recv #~p: ~p~n",
				[QueryNo, [Address || #{address := Address} <- Peers]]
			),
			{Line, Mappings};
		{noreply, Ref} ->
			QueryNo = maps:get(Ref, Mappings),
			Line = io_lib:format(
				"fail #~p~n",
				[QueryNo]
			),
			{Line, Mappings}
	end,
	format_history(Rest, NewMappings, [NewLine | Acc]).
