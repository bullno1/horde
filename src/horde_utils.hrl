-define(RECORD_TO_MAP(Type, Record),
	horde_utils:record_to_map(record_info(fields, Type), Record)).
