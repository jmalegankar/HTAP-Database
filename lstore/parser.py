def parse_rid(rid, position, length):
	return (rid // (10 ** position)) % (10 ** length)

def get_physical_page_offset(rid):
	return parse_rid(rid, 0, 3) # starting from digit 0, length 3