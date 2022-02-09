#!/usr/bin/env python3

from time import process_time

list_avg_insert = 0
set_avg_insert = 0

list_avg_delete = 0
set_avg_delete = 0

set_to_list = 0


for i in range(10):
    my_list = []
    start = process_time()
    for j in range(10000):
        my_list.append(j)
    list_avg_insert += (process_time() - start)
    

for i in range(10):
    my_list = []
    for j in range(10000):
        my_list.append(j)
    
    start = process_time()
    for j in range(10000 - 1, -1, -1):
        my_list.remove(j)
    list_avg_delete += (process_time() - start)



for i in range(10):
    my_set = set()
    start = process_time()
    for j in range(10000):
        my_set.add(j)
    set_avg_insert += (process_time() - start)

for i in range(10):
    my_set = set()
    for j in range(10000):
        my_set.add(j)
    
    start = process_time()
    for j in range(10000 - 1, -1, -1):
        my_set.remove(j)
    set_avg_delete += (process_time() - start)

for i in range(10):
    my_set = set()
    for j in range(10000):
        my_set.add(j)
    
    start = process_time()
    list_from_set = list(my_set)
    set_to_list += (process_time() - start)

print('LIST INSERT:\t', (list_avg_insert/10))
print('SET INSERT:\t', (set_avg_insert/10))

print('LIST DELETE:\t', (list_avg_delete/10))
print('SET DELETE:\t', (set_avg_delete/10))

print('SET TO LIST:\t', (set_to_list/10))