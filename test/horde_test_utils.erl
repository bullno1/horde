-module(horde_test_utils).
-export([
	create_node/0,
	create_node/1,
	with_tracer/2,
	lookup_traced/4,
	lookup_traced/5,
	report_lookup_history/1
]).

% API

create_node() ->
	create_node(#{
		address => horde_addr_gen:next_address(),
		max_address => horde_addr_gen:max_address()
	}).

create_node(Opts) ->
	DefaultOpts = #{
		transport => {horde_disterl, #{active => false}}
	},
	{ok, Node} = horde:start_link(maps:merge(DefaultOpts, Opts)),
	Node.

with_tracer(TracedFun, ProcessTraceFun) ->
	Tracer = horde_tracer:new(),
	Result = TracedFun(Tracer),
	ProcessTraceFun(Result, horde_tracer:collect_events(Tracer)),
	Result.

lookup_traced(Ref, Address, ReportOnSucess, ReportOnFailure) ->
	lookup_traced(Ref, Address, #{}, ReportOnSucess, ReportOnFailure).

lookup_traced(Ref, Address, Opts, ReportOnSucess, ReportOnFailure) ->
	with_tracer(
		fun(Tracer) -> horde:lookup(Ref, Address, Opts#{tracer => Tracer}) end,
		fun
			({ok, _}, History) when ReportOnSucess ->
				report_lookup_history(History);
			(error, History) when ReportOnFailure ->
				report_lookup_history(History);
			(_, _) ->
				ok
		end
	).

report_lookup_history(History) ->
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
