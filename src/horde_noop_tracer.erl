-module(horde_noop_tracer).
-behaviour(horde_tracer).
-export([init/1, handle_event/2]).

init(_) -> [].

handle_event(_, State) -> State.
