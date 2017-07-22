-module(ring_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").

-define(MAX_KEY, 15).

all() -> [{group, kv}, neighbours].

groups() ->
	[{kv, [parallel], [lookup, update, insert, remove]}].

rev_fun(K) -> ?MAX_KEY - K.

init_per_suite(Config) ->
	[{ring, horde_ring:new(fun rev_fun/1)} | Config].

end_per_suite(_) -> ok.

init_per_group(kv, Config) ->
	Ring = ?config(ring, Config),
	Ring2 = horde_ring:insert(6, {?MODULE, 6}, Ring),
	Ring3 = horde_ring:insert(9, {?MODULE, 9}, Ring2),
	lists:keyreplace(ring, 1, Config, {ring, Ring3}).

end_per_group(_, _) -> ok.

lookup(Config) ->
	Ring = ?config(ring, Config),
	{value, {?MODULE, 6}} = horde_ring:lookup(6, Ring),
	{value, {?MODULE, 9}} = horde_ring:lookup(9, Ring),
	none = horde_ring:lookup(15, Ring),
	ok.

update(Config) ->
	Ring = ?config(ring, Config),

	Ring2 = horde_ring:update(6, {?MODULE, new}, Ring),
	{value, {?MODULE, new}} = horde_ring:lookup(6, Ring2),

	error =
		try horde_ring:update(8, {?MODULE, new}, Ring) of
			_ -> ok
		catch
			_:_ -> error
		end,

	ok.

insert(Config) ->
	Ring = ?config(ring, Config),

	Ring2 = horde_ring:insert(8, {?MODULE, new}, Ring),
	{value, {?MODULE, new}} = horde_ring:lookup(8, Ring2),

	error =
		try horde_ring:insert(6, {?MODULE, new}, Ring) of
			_ -> ok
		catch
			_:_ -> error
		end,

	ok.

remove(Config) ->
	Ring = ?config(ring, Config),

	Ring2 = horde_ring:remove(6, Ring),
	none = horde_ring:lookup(6, Ring2),
	{value, {?MODULE, 9}} = horde_ring:lookup(9, Ring2),

	Ring = horde_ring:remove(10, Ring),

	ok.

neighbours(Config) ->
	EmptyRing = ?config(ring, Config),
	Ring = lists:foldl(
		fun(Key, Acc) -> horde_ring:insert(Key, {?MODULE, Key}, Acc) end,
		EmptyRing,
		[2, 4, 6, 8]
	),

	[] = horde_ring:successors(69, 2, EmptyRing),
	[] = horde_ring:predecessors(69, 2, EmptyRing),
	[] = horde_ring:successors(69, 0, Ring),
	[] = horde_ring:predecessors(69, 0, Ring),

	[{?MODULE, 2}, {?MODULE, 4}, {?MODULE, 8}] = horde_ring:successors(7, 3, Ring),
	[{?MODULE, 2}, {?MODULE, 4}, {?MODULE, 6}, {?MODULE, 8}] = horde_ring:successors(7, 10, Ring),
	[{?MODULE, 2}, {?MODULE, 4}, {?MODULE, 8}] = horde_ring:successors(6, 3, Ring),
	[{?MODULE, 2}, {?MODULE, 4}, {?MODULE, 8}] = horde_ring:successors(6, 10, Ring),
	[{?MODULE, 2}, {?MODULE, 4}] = horde_ring:successors(9, 2, Ring),
	[{?MODULE, 2}, {?MODULE, 4}] = horde_ring:successors(1, 2, Ring),

	[{?MODULE, 2}, {?MODULE, 6}, {?MODULE, 8}] = horde_ring:predecessors(3, 3, Ring),
	[{?MODULE, 4}, {?MODULE, 6}, {?MODULE, 8}] = horde_ring:predecessors(2, 10, Ring),

	% Ring2 = [2, 6, 7, 8]
	Ring2 = horde_ring:remove(4, horde_ring:insert(7, {?MODULE, 7}, Ring)),
	[{?MODULE, 2}, {?MODULE, 7}, {?MODULE, 8}] = horde_ring:predecessors(3, 3, Ring2),
	[{?MODULE, 2}, {?MODULE, 8}] = horde_ring:successors(7, 2, Ring2),

	ok.
