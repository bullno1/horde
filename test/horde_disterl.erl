-module(horde_disterl).
-behaviour(horde_transport).
-behaviour(gen_server).
% horde_transport
-export([
	open/2,
	info/2,
	send/4,
	recv_async/1,
	close/1
]).
% gen_server
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2
]).

-record(state, {
	ctx :: horde_transport:ctx(),
	address :: horde:compound_address(),
	active = false :: boolean() | once
}).

% horde_transport
open(Ctx, Opts) -> gen_server:start_link(?MODULE, {Ctx, Opts}, []).

info(Ref, Info) -> gen_server:call(Ref, {info, Info}).

send(Ref, Address, Id, Body) -> gen_server:cast(Ref, {send, Address, Id, Body}).

recv_async(Ref) -> gen_server:cast(Ref, recv_async).

close(Ref) -> gen_server:call(Ref, close).

% gen_server

init({Ctx, Opts}) ->
	State = #state{
		ctx = Ctx,
		address = horde_transport:compound_address(Ctx, self()),
		active = maps:get(active, Opts, false)
	},
	{ok, State}.

handle_call({info, address}, _, State) ->
	{reply, self(), State};
handle_call(close, _, State) ->
	{stop, normal, ok, State}.

handle_cast({send, Address, Id, Body}, #state{address = OwnAddress} = State) ->
	Header = #{
		from => OwnAddress,
		id => Id,
		timestamp => erlang:system_time(seconds)
	},
	Address ! {?MODULE, {message, Header, Body}},
	{noreply, State};
handle_cast(recv_async, #state{active = false} = State) ->
	{noreply, State#state{active = once}};
handle_cast(recv_async, State) ->
	{noreply, State}.

handle_info({?MODULE, _Event}, #state{active = false} = State) ->
	{noreply, State};
handle_info({?MODULE, Event}, #state{active = Active, ctx = Ctx} = State) ->
	NextActive =
		case Active of
			true -> true;
			once -> false
		end,
	horde_transport:notify(Ctx, self(), Event),
	{noreply, State#state{active = NextActive}}.
