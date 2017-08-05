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

% API

start_link(Opts) -> gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

stop() -> gen_server:stop(?MODULE).

% gen_server

init(Opts) -> {ok, apply(rand, seed_s, Opts)}.

handle_call(max_address, _, State) ->
	{reply, 65535, State};
handle_call(generate_keypair, _, State) ->
	{PrivKey, State2} = rand:uniform_s(65535, State),
	{PubKey, State3} = rand:uniform_s(65535, State2),
	{reply, {PubKey, PrivKey}, State3}.

handle_cast(_, State) -> {noreply, State}.
