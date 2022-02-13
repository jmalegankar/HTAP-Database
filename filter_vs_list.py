import timeit
modules = '''
from collections import defaultdict
test = defaultdict(list)
for i in range(10000):
    test[i] = [i*2]
'''

filter_code = '''
def test_function():
    return [test[primary_key][0] for primary_key in filter(lambda x: 2500 <= x <= 7500, test)]
test_function()
'''

list_code = '''
def test_function():
    rids = []
    for key, rid in test.items():
        if 2500 <= key <= 7500:
            rids.append(rid[0])
    return rids
test_function()
'''

list_code2 = '''
def test_function():
    rids = []
    for key in test:
        if 2500 <= key <= 7500:
            rids.append(test[key][0])
    return rids
test_function()
'''

#print(timeit.timeit(stmt=filter_code, setup=modules, number=10000))
print(timeit.timeit(stmt=list_code, setup=modules, number=20000))
print(timeit.timeit(stmt=list_code2, setup=modules, number=20000))