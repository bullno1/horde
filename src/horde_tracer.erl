-module(horde_tracer).
-behaviour(gen_server).
% API
-export([
	new/0,
	noop/0,
	handle_event/2,
	collect_events/1
]).
% gen_server
-export([
	init/1,
	handle_call/3,
	handle_cast/2
]).
-export_type([tracer/0]).

-opaque tracer() :: pid() | noop.

% API

-spec new() -> tracer().
new() ->
	{ok, Tracer} = gen_server:start_link(?MODULE, [], []),
	Tracer.

-spec handle_event(term(), tracer()) -> ok.
handle_event(_Event, noop) ->
	ok;
handle_event(Event, Pid) ->
	gen_server:cast(Pid, {handle_event, Event}).

-spec noop() -> tracer().
noop() -> noop.

-spec collect_events(tracer()) -> list().
collect_events(noop) ->
	[];
collect_events(Pid) ->
	gen_server:call(Pid, collect_events).

% gen_server

init([]) -> {ok, []}.

handle_call(collect_events, _From, Events) ->
	{stop, normal, lists:reverse(Events), []}.

handle_cast({handle_event, Event}, Events) ->
	{noreply, [Event | Events]}.
