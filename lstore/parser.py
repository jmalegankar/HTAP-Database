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

def create_rid(type, range_number, page_number, offset):
	return type * (10 ** 8) + range_number * (10 ** 6) + page_number * (10 ** 3) + offset
