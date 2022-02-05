from lstore.pageRange import PageRange
import time
import sys

page_range_0 = PageRange(3, 0)

page_range_0.write(0, 1, 2)

start = time.process_time()
page_range_0.get_withRID(0,[1,1,1])
end = time.process_time()
print('0 Update Lookup Took: ', end - start)


page_range_0.update(0, 3, None, None)

start = time.process_time()
page_range_0.get_withRID(0,[1,1,1])
end = time.process_time()

print('1 Update Lookup Took: ', end - start)


sys.setrecursionlimit(99999)

start_update = time.process_time()
for i in range(1000000):
	page_range_0.update(0, i, None, None)
page_range_0.update(0, i, 10, None)
end_update = time.process_time()
print('1M Update Took: ', end_update - start_update)



# Read doesn't work for non-cumulative version

start = time.process_time()
page_range_0.get_withRID(0,[1,1,1])
end = time.process_time()
print('1M Update Lookup Took: ', end - start)

print(page_range_0)
