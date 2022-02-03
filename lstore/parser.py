def parse_rid(rid, position, length):
	return (rid // (10 ** position)) % (10 ** length)

def get_page_type(rid):
	return parse_rid(rid, 8, 1)

def get_page_range_number(rid):
	return parse_rid(rid, 6, 2)

def get_page_number(rid):
	return parse_rid(rid, 3, 3)

def get_physical_page_offset(rid):
	return parse_rid(rid, 0, 3) # starting from digit 0, length 3

def create_rid(type, range_number, page_number, offset):
	return type * (10 ** 8) + range_number * (10 ** 6) + page_number * (10 ** 3) + offset


if __name__ == '__main__':
	assert get_physical_page_offset(123456789) == 789
	assert get_page_type(123456789) == 1
	assert get_page_range_number(123456789) == 23
	assert get_page_number(123456789) == 456
	
	assert get_page_type(create_rid(1, 0, 0, 0)) == 1
	assert get_page_range_number(create_rid(0, 23, 0, 0)) == 23
	assert get_page_number(create_rid(0, 0, 456, 0)) == 456
	assert get_physical_page_offset(create_rid(0, 0, 0, 789)) == 789
	print(create_rid(1, 23, 456, 789))
