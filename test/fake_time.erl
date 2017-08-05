-module(fake_time).
-behaviour(gen_server).
% API
-export([
	start_link/0,
	stop/0,
	send_after/3,
	start_timer/3,
	cancel_timer/1,
	cancel_timer/2,
	apply_policy/2,
	combine_policies/1,
	set_timer_policy/1,
	with_policy/2,
	trigger_timers/0,
	trigger_timer/1,
	process_timers/1
]).
% gen_server
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2
]).
-export_type([timer/0, timer_policy/0]).

-type timer() :: {Ref :: reference(), Target :: atom() | pid(), Msg :: term()}.
-type timer_policy()
	:: stateless_timer_policy()
	 | stateful_timer_policy()
	 | timer_policy_action().
-type stateless_timer_policy() :: fun((timer()) -> timer_policy_action()).
-type stateful_timer_policy() :: {stateful_timer_policy_fun(), State :: term()}.
-type stateful_timer_policy_fun()
	:: fun((timer(), State) -> {timer_policy_action(), State}).
-type timer_policy_action() :: drop | delay | trigger.

-record(state, {
	timers = #{},
	policy = delay :: timer_policy()
}).

% API

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() -> gen_server:stop(?MODULE).

send_after(_Timeout, Target, Message) ->
	TimerRef = make_ref(),
	Timer = {TimerRef, Target, Message},
	gen_server:cast(?MODULE, {start_timer, Timer}),
	TimerRef.

start_timer(_Timeout, Target, Message) ->
	TimerRef = make_ref(),
	Timer = {TimerRef, Target, {timeout, TimerRef, Message}},
	gen_server:cast(?MODULE, {start_timer, Timer}),
	TimerRef.

cancel_timer(Ref) -> gen_server:cast(?MODULE, {cancel_timer, Ref}).

cancel_timer(Ref, _) -> cancel_timer(Ref).

trigger_timers() ->
	process_timers(trigger).

trigger_timer(Timer) ->
	process_timers(
		fun({Ref, _, _}) ->
			case Ref =:= Timer of
				true -> trigger;
				false -> delay
			end
		end
	).

-spec process_timers(timer_policy()) -> ok.
process_timers(Policy) -> gen_server:call(?MODULE, {process_timers, Policy}).

-spec set_timer_policy(timer_policy()) -> timer_policy().
set_timer_policy(Policy) ->
	gen_server:call(?MODULE, {set_timer_policy, Policy}).

-spec with_policy(timer_policy(), fun(() -> X)) -> X.
with_policy(Policy, Fun) ->
	OldPolicy = set_timer_policy(Policy),
	try Fun() of
		X -> X
	after
		set_timer_policy(OldPolicy)
	end.

-spec apply_policy(timer_policy(), timer()) -> {timer_policy_action(), timer_policy()}.
apply_policy(Policy, _Timer) when is_atom(Policy) ->
	{Policy, Policy};
apply_policy(Policy, Timer) when is_function(Policy, 1) ->
	{Policy(Timer), Policy};
apply_policy({PolicyFun, PolicyState}, Timer) when is_function(PolicyFun, 2) ->
	{PolicyAction, NewPolicyState} = PolicyFun(Timer, PolicyState),
	{PolicyAction, {PolicyFun, NewPolicyState}}.

-spec combine_policies([timer_policy()]) -> timer_policy().
combine_policies(Policies) -> {fun apply_policies/2, Policies}.

% gen_server

init([]) ->
	process_flag(trap_exit, true),
	{ok, #state{}}.

handle_call({process_timers, Policy}, _, #state{timers = Timers} = State) ->
	{NewTimers, _NewPolicy} = process_timers(Policy, Timers),
	{reply, ok, State#state{timers = NewTimers}};
handle_call(
	{set_timer_policy, NewPolicy}, _,
	#state{policy = OldPolicy, timers = Timers} = State
) ->
	{NewTimers, NewPolicy2} = process_timers(NewPolicy, Timers),
	{reply, OldPolicy, State#state{policy = NewPolicy2, timers = NewTimers}}.

handle_cast({start_timer, Timer}, State) ->
	{noreply, add_timer(Timer, State)};
handle_cast({cancel_timer, Ref}, #state{timers = Timers} = State) ->
	{noreply, State#state{timers = maps:remove(Ref, Timers)}}.

handle_info({'EXIT', Process, _}, #state{timers = Timers} = State) ->
	Timers2 = maps:filter(
		fun(_, {_, Pid, _}) -> Pid =/= Process end,
		Timers
	),
	{noreply, State#state{timers = Timers2}};
handle_info(_, State) ->
	{noreply, State}.

% Private

add_timer(
	{Ref, Process, _} = Timer,
	#state{timers = Timers, policy = Policy} = State
) ->
	link(Process),
	{PolicyAction, NewPolicy} = apply_policy(Policy, Timer),
	State2 = State#state{policy = NewPolicy},
	case PolicyAction of
		drop -> State2;
		delay -> State2#state{timers = maps:put(Ref, Timer, Timers)};
		trigger -> send_timer(Timer), State2
	end.

apply_policies(Timer, Policies) -> apply_policies(Timer, Policies, []).

apply_policies(_Timer, [], Acc) ->
	{delay, lists:reverse(Acc)};
apply_policies(Timer, [Policy | Rest], Acc) ->
	{PolicyAction, NewPolicy} = apply_policy(Policy, Timer),
	case PolicyAction of
		drop ->
			{drop, lists:reverse([NewPolicy | Acc], Rest)};
		delay ->
			apply_policies(Timer, Rest, [NewPolicy | Acc]);
		trigger ->
			{trigger, lists:reverse([NewPolicy | Acc], Rest)}
	end.

process_timers(Policy, Timers) ->
	maps:fold(
		fun(Ref, Timer, {NewTimers, CurrentPolicy}) ->
			case apply_policy(CurrentPolicy, Timer) of
				{drop, NewPolicy} ->
					{NewTimers, NewPolicy};
				{delay, NewPolicy} ->
					{maps:put(Ref, Timer, NewTimers), NewPolicy};
				{trigger, NewPolicy} ->
					send_timer(Timer),
					{NewTimers, NewPolicy}
			end
		end,
		{maps:new(), Policy},
		Timers
	).

send_timer({_, Target, Message}) -> Target ! Message.
