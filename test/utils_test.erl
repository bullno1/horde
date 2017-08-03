-module(utils_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

prop_test_() ->
	?_assert([] =:= proper:check_specs(horde_address, [{to_file, user}])).
