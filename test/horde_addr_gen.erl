-module(horde_addr_gen).
-behaviour(gen_server).
% API
-export([
	start_link/1,
	reset/1,
	next_address/0,
	max_address/0,
	stop/0
]).
% gen_server
-export([
	init/1,
	handle_call/3,
	handle_cast/2
]).
-record(state, {
	rand,
	addresses = sets:new()
}).

% API

start_link(Opts) -> gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

reset(Opts) -> gen_server:call(?MODULE, {reset, Opts}).

max_address() -> 65535.

next_address() -> gen_server:call(?MODULE, next_address).

stop() -> gen_server:stop(?MODULE).

% gen_server

init(Seed) -> {ok, #state{rand = apply(rand, seed_s, Seed)}}.

handle_call({reset, Seed}, _, _State) ->
	{reply, ok, #state{rand = apply(rand, seed_s, Seed)}};
handle_call(next_address, _, State) ->
	{Address, State2} = next_address(State),
	{reply, Address, State2}.

handle_cast(_, State) -> {noreply, State}.

% Private

next_address(#state{rand = Rand, addresses = Addresses} = State) ->
	{Address, Rand2} = rand:uniform_s(max_address(), Rand),
	case sets:is_element(Address, Addresses) of
		true ->
			next_address(State#state{rand = Rand2});
		false ->
			State2 = State#state{
				rand = Rand2,
				addresses = sets:add_element(Address, Addresses)
			},
			{Address, State2}
	end.
