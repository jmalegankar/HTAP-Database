"""
RID format: abbcccddd
	a: page type
		0: base page
		1: tail page
		2: invalid record (deleted)
	bb: page range number
	ccc: page number
	ddd: physical page offset (rec_num)
"""

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

def create_rid(page_type, range_number, page_number, offset):
	return page_type * (100000000) + range_number * (1000000) + page_number * (1000) + offset

def get_page_number_and_offset(rid):
	last_six = rid % 1000000
	return (last_six // 1000, last_six % 1000)
