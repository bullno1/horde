-module(horde_disterl).
-include_lib("stdlib/include/assert.hrl").
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
	msg_q = queue:new() :: queue:queue(),
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
	verify_msg(Body),
	Header = #{
		from => OwnAddress,
		id => Id,
		timestamp => erlang:unique_integer([positive, monotonic])
	},
	Address ! {?MODULE, {message, Header, Body}},
	{noreply, State};
handle_cast(recv_async, #state{ctx = Ctx, active = false, msg_q = MsgQ} = State) ->
	case queue:out(MsgQ) of
		{{value, Event}, MsgQ2} ->
			horde_transport:notify(Ctx, self(), Event),
			{noreply, State#state{msg_q = MsgQ2}};
		{empty, _} ->
			{noreply, State#state{active = once}}
	end;
handle_cast(recv_async, State) ->
	{noreply, State}.

handle_info({?MODULE, Event}, #state{ctx = Ctx, active = true} = State) ->
	horde_transport:notify(Ctx, self(), Event),
	{noreply, State};
handle_info({?MODULE, Event}, #state{active = false, msg_q = MsgQ} = State) ->
	{noreply, State#state{msg_q = queue:in(Event, MsgQ)}};
handle_info({?MODULE, Event}, #state{active = once, ctx = Ctx} = State) ->
	horde_transport:notify(Ctx, self(), Event),
	{noreply, State#state{active = false}}.

% Private

verify_msg({peer_info, Peers}) ->
	_ = [?assertEqual(error, maps:find(source, Peer)) || Peer <- Peers],
	Addresses = [OverlayAddress || #{address := #{overlay := OverlayAddress}} <- Peers],
	Length = length(Addresses),
	Length2 = length(lists:usort(Addresses)),
	case Length =:= Length2 of
		true ->
			ok;
		false ->
			ct:pal("Peers = ~p", [Peers]),
			exit(duplicate)
	end;
verify_msg(_) ->
	ok.
