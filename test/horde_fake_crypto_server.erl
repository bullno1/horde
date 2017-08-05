-module(horde_fake_crypto_server).
-behaviour(gen_server).
% API
-export([
	start_link/1,
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
	keys = sets:new()
}).
-define(MAX_KEY, 65535).

% API

start_link(Opts) -> gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

stop() -> gen_server:stop(?MODULE).

% gen_server

init(Seed) -> {ok, #state{rand = apply(rand, seed_s, Seed)}}.

handle_call(max_address, _, State) ->
	{reply, ?MAX_KEY, State};
handle_call({reset_seed, Seed}, _, State) ->
	{reply, ok, State#state{rand = apply(rand, seed_s, Seed)}};
handle_call(generate_keypair, _, State) ->
	{KeyPair, State2} = generate_keypair(State),
	{reply, KeyPair, State2}.

handle_cast(_, State) -> {noreply, State}.

% Private

generate_keypair(#state{rand = Rand, keys = Keys} = State) ->
	{PrivKey, Rand2} = rand:uniform_s(?MAX_KEY, Rand),
	KeyPair = {PrivKey - 1, PrivKey},
	case sets:is_element(KeyPair, Keys) of
		true ->
			generate_keypair(State#state{rand = Rand2});
		false ->
			State2 = State#state{
				rand = Rand2,
				keys = sets:add_element(KeyPair, Keys)
			},
			{KeyPair, State2}
	end.
