-module(horde_ring).
-compile({no_auto_import, [size/1]}).
-export([
	new/1,
	successors/3,
	predecessors/3,
	lookup/2,
	update/3,
	insert/3,
	remove/2
]).
-export_type([ring/2, rev_fun/1]).
-record(ring, {rev_fun, forward, reverse}).

-opaque ring(K, V) :: #ring{
	rev_fun :: rev_fun(K),
	forward :: gb_trees:tree(K, V),
	reverse :: gb_trees:tree(K, V)
}.
-type rev_fun(T) :: fun((T) -> T).

% API

-spec new(rev_fun(K)) -> ring(K, _).
new(RevFun) ->
	#ring{
		rev_fun = RevFun,
		forward = gb_trees:empty(),
		reverse = gb_trees:empty()
	}.

-spec successors(Key, non_neg_integer(), ring(Key, Value)) ->
	ordsets:ordset(Value).
successors(Key, Num, #ring{forward = ForwardTree}) ->
	case gb_trees:size(ForwardTree) =:= 0 orelse Num =:= 0 of
		true -> [];
		false -> collect_n(Num, Key, ForwardTree)
	end.

-spec predecessors(Key, non_neg_integer(), ring(Key, Value)) ->
	ordsets:ordset(Value).
predecessors(Key, Num, #ring{rev_fun = RevFun, reverse = ReverseTree}) ->
	case gb_trees:size(ReverseTree) =:= 0 orelse Num =:= 0 of
		true -> [];
		false -> collect_n(Num, RevFun(Key), ReverseTree)
	end.

-spec lookup(Key, ring(Key, Value)) -> {value, Value} | none.
lookup(Key, #ring{forward = ForwardTree}) -> gb_trees:lookup(Key, ForwardTree).

-spec update(Key, Value, ring(Key, Value)) -> ring(Key, Value).
update(Key, Value, Ring) ->
	modify_trees(Key, fun(K, Tree) -> gb_trees:update(K, Value, Tree) end, Ring).

-spec insert(Key, Value, ring(Key, Value)) -> ring(Key, Value).
insert(Key, Value, Ring) ->
	modify_trees(Key, fun(K, Tree) -> gb_trees:insert(K, Value, Tree) end, Ring).

-spec remove(Key, ring(Key, Value)) -> ring(Key, Value).
remove(Key, Ring) ->
	modify_trees(Key, fun(K, Tree) -> gb_trees:delete_any(K, Tree) end, Ring).

% Private

collect_n(N, FromKey, Tree) ->
	case gb_trees:next(gb_trees:iterator_from(FromKey, Tree)) of
		{FromKey, _Value, Iter} ->
			collect_n(N, Iter, FromKey, Tree, []);
		{_Key, Value, Iter} ->
			collect_n(N - 1, Iter, FromKey, Tree, [Value]);
		none ->
			collect_n(N, gb_trees:iterator(Tree), FromKey, Tree, [])
	end.

collect_n(0, _Iter, _End, _Tree, Acc) ->
	Acc;
collect_n(N, Iter, End, Tree, Acc) ->
	case gb_trees:next(Iter) of
		{End, _Value, _Iter2} ->
			Acc;
		{_Key, Value, Iter2} ->
			collect_n(N - 1, Iter2, End, Tree, ordsets:add_element(Value, Acc));
		none ->
			collect_n(N, gb_trees:iterator(Tree), End, Tree, Acc)
	end.

modify_trees(
	Key, Fun,
	#ring{
		rev_fun = RevFun, forward = ForwardTree, reverse = ReverseTree
	} = Ring
) ->
	Ring#ring{
		forward = Fun(Key, ForwardTree),
		reverse = Fun(RevFun(Key), ReverseTree)
	}.
