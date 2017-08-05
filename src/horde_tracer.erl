-module(horde_tracer).
-export([new/2, handle_event/2, noop/0]).
-export_type([tracer/0]).

-opaque tracer() :: {module(), term()}.

-callback init(Opts) -> State when
	  Opts :: term(),
	  State :: term().

-callback handle_event(Event, State) -> NewState when
	  Event :: term(),
	  State :: term(),
	  NewState :: term().

-spec new(module(), term()) -> tracer().
new(Module, Opts) -> {Module, Module:init(Opts)}.

-spec handle_event(term(), tracer()) -> tracer().
handle_event(Event, {Module, State}) ->
	NewState = Module:handle_event(Event, State),
	{Module, NewState}.

-spec noop() -> tracer().
noop() -> new(horde_noop_tracer, []).
