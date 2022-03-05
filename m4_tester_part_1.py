from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed

import shutil
try:
    shutil.rmtree('./ECS165')
except:
    pass

db = Database()
db.open('./ECS165')
grades_table = db.create_table('Grades', 5, 0)
num_threads = 8

try:
    grades_table.index.create_index(1)
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not implemented properly, tests may fail.')

keys = []
records = {}
seed(3562901)

insert_transactions = []
transaction_workers = []
for i in range(num_threads):
    insert_transactions.append(Transaction())
    transaction_workers.append(TransactionWorker())
    transaction_workers[i].add_transaction(insert_transactions[i])

for i in range(0, 1000):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    q = Query(grades_table)
    t = insert_transactions[i % num_threads]
    t.add_query(q.insert, grades_table, *records[key])

# Commit to disk
for i in range(num_threads):
    transaction_workers[i].run()

for i in range(num_threads):
    transaction_workers[i].join()

for i in range(num_threads):
    if transaction_workers[i].result != len(transaction_workers[i].transactions):
        print('Something is wrong with transaction_workers', i)

db.close()