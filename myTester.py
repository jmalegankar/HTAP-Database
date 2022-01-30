from lstore.pageRange import PageRange
import time




#tester
# the file i used to test the implmentations of both Pagerange
# and page
start = time.time()
x= PageRange(2)
x.create_a_new_base_page()
x.create_a_new_base_page(True)
off=0
z=0
for y in range(512):
    x.write(0,[512-y,y])
    off+=8
print(x.GET_record(0,[4,5],0))

print(time.time() - start)