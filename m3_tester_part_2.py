from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed

db = Database()
db.open('./database')
# Getting the existing Grades table
grades_table = db.get_table('Grades')

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_transactions = 100
number_of_operations_per_record = 10
num_threads = 8

keys = []
records = {}
seed(3562901)

# re-generate records for testing
for i in range(0, number_of_records):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]

transaction_workers = []
transactions = []

for i in range(number_of_transactions):
    transactions.append(Transaction())

for i in range(num_threads):
    transaction_workers.append(TransactionWorker())


"""
for i in range(0, number_of_records):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    q = Query(grades_table)
"""

# x update on every column
for j in range(number_of_operations_per_record):
    for key in keys:
        updated_columns = [None, None, None, None, None]
        for i in range(2, grades_table.num_columns):
            # updated value
            value = randint(0, 20)
            updated_columns[i] = value
            # copy record to check
            original = records[key].copy()
            # update our test directory
            records[key][i] = value
            transactions[j % number_of_transactions].add_query(query.select, grades_table, key, 0, [1, 1, 1, 1, 1])
            transactions[j % number_of_transactions].add_query(query.update, grades_table, key, *updated_columns)
            # print('transactions', j % number_of_transactions, 'added update query', key, *updated_columns)
#           print((j % number_of_transactions), key, updated_columns)
print("Update finished")


# add trasactions to transaction workers  
for i in range(number_of_transactions):
    transaction_workers[i % num_threads].add_transaction(transactions[i])



# run transaction workers 
for i in range(num_threads):
    transaction_workers[i].run()

# wait for workers to finish
for i in range(num_threads):
    transaction_workers[i].join()

for i in range(num_threads):
    # print(transaction_workers[i].result, 'ok, we have',  len(transaction_workers[i].transactions))
    if transaction_workers[i].result != len(transaction_workers[i].transactions):
        print('[A] Something is wrong with transaction_workers', i)



score = len(keys)
for key in keys:
    correct = records[key]
    query = Query(grades_table)
    
    result = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    if correct != result.columns:
        print('select error on primary key', key, ':', result, ', correct:', correct)
        score -= 1
    # else:
    #     print('primary key', key, 'ok!')
print('Score', score, '/', len(keys))

# print(grades_table.lock_manager)


delete_transaction_workers = []
delete_transactions = []

# Delete all records
for i in range(number_of_transactions):
    delete_transactions.append(Transaction())

for i in range(num_threads):
    delete_transaction_workers.append(TransactionWorker())

query = Query(grades_table)

for i, key in enumerate(keys):
    delete_transactions[i % number_of_transactions].add_query(query.delete, grades_table, key)

for i in range(number_of_transactions):
    delete_transaction_workers[i % num_threads].add_transaction(delete_transactions[i])

# run transaction workers 
for i in range(num_threads):
    delete_transaction_workers[i].run()

# wait for workers to finish
for i in range(num_threads):
    delete_transaction_workers[i].join()

for i in range(num_threads):
    # print(delete_transaction_workers[i].result, 'ok, we have',  len(delete_transaction_workers[i].transactions))
    if delete_transaction_workers[i].result != len(delete_transaction_workers[i].transactions):
        print('[B] Something is wrong with transaction_workers', i)

query = Query(grades_table)
for key in keys:
    result = query.select(key, 0, [1, 1, 1, 1, 1])
    if len(result) > 0:
        print('delete error on primary key', key, ':', result, 'expected: []')
print('delete OK!')
"""
"""

db.close()
