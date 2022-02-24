#!/usr/bin/env python3

import unittest
from lstore.db import Database
from lstore.table import Table
from lstore.query import Query
import time
import random
import shutil
import sys

class ExtendedTester(unittest.TestCase):
    
    def setUp(self):
        seed = random.randrange(sys.maxsize)
        print(str(seed) + '\n')
        try:
            shutil.rmtree('./database')
        except:
            pass
    """
    Create index for column 1, 2, 3, 4
    Insert 10k objects
    Select 10k objects randomly using random colummn value
    Update 10k objects randomly and select it
    Select 10k objects randomly using random column value
    Shutdown database and select 10k objects randomly using random colummn value
    Update 10k objects randomly and select it
    """
    def test_update(self):
        for i in range(100):
            self.setUp()
            print(i)
            self.update()
            print('{} OK'.format(i))
            
    def update(self):	
        db = Database()
        db.open('./database')
        table = db.create_table('update', 5, 0)
        for i in range(1, 5):
            table.index.create_index(i)
        query = Query(table)
        
        records = []
        index = [i for i in range(10000)]
        random.shuffle(index)
        
        keys = random.sample(range(-100000000, 100000001), 10000)
        
        for key in keys:
            record = [key, random.randint(-100000000, 100000000), random.randint(-100000000, 100000000), random.randint(-100000000, 100000000), random.randint(-100000000, 100000000)]
            records.append(record)
            self.assertTrue(query.insert(*record))
            
        for i in index:
            correct = records[i]
            random_index = random.randint(0, 4)
            results = query.select(correct[random_index], random_index, [1] * 5)
            if len(results) > 0:
                # need to check one by one
                ok = False
                for result in results:
                    if result.columns == correct:
                        ok = True
                self.assertTrue(ok)
            else:
                self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)
                
        random.shuffle(index)
        for i in index:
            primary_key = records[i][0]
            update = [None, None, None, None, None]
            update_bit = [False, False, False, False, False]
            # randomly select columns to update
            how_many_columns = random.randint(1, 5)
            for j in range(how_many_columns):
                update_bit[random.randint(0, 4)] = True
                
            for idx, do_update in enumerate(update_bit):
                if do_update:
                    if idx == 0:
                        update[idx] = records[i][idx] + 1000000000
                    else:
                        update[idx] = random.randint(-100000000, 100000000)
                    records[i][idx] = update[idx]
            self.assertTrue(query.update(primary_key, *update))
            
            # test select after update
            correct = records[i]
            random_index = random.randint(0, 4)
            results = query.select(correct[random_index], random_index, [1] * 5)
            if len(results) > 0:
                # need to check one by one
                ok = False
                for result in results:
                    if result.columns == correct:
                        ok = True
                self.assertTrue(ok)
            else:
                self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)
#       time.sleep(1)
        # test select ALL after update
        random.shuffle(index)
        for i in index:
            correct = records[i]
            random_index = random.randint(0, 4)
            results = query.select(correct[random_index], random_index, [1] * 5)
            if len(results) > 0:
                # need to check one by one
                ok = False
                for result in results:
                    if result.columns == correct:
                        ok = True
                self.assertTrue(ok)
            else:
                self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)
                
        db.close()
        del db, table, query
        
        random.shuffle(index)
        
        db = Database()
        db.open('./database')
        table = db.get_table('update')
        query = Query(table)
        
        for i in index:
            correct = records[i]
            random_index = random.randint(0, 4)
            results = query.select(correct[random_index], random_index, [1] * 5)
            if len(results) > 0:
                # need to check one by one
                ok = False
                for result in results:
                    if result.columns == correct:
                        ok = True
                self.assertTrue(ok)
            else:
                self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)
                
                
        random.shuffle(index)
        for i in index:
            primary_key = records[i][0]
            update = [None, None, None, None, None]
            update_bit = [False, False, False, False, False]
            # randomly select columns to update
            how_many_columns = random.randint(1, 5)
            for j in range(how_many_columns):
                update_bit[random.randint(0, 4)] = True
                
            for idx, do_update in enumerate(update_bit):
                if do_update:
                    if idx == 0:
                        update[idx] = records[i][idx] + 1000000000
                    else:
                        update[idx] = random.randint(-100000000, 100000000)
                    records[i][idx] = update[idx]
            self.assertTrue(query.update(primary_key, *update))
            
            # test select after update
            correct = records[i]
            random_index = random.randint(0, 4)
            results = query.select(correct[random_index], random_index, [1] * 5)
            if len(results) > 0:
                # need to check one by one
                ok = False
                for result in results:
                    if result.columns == correct:
                        ok = True
                self.assertTrue(ok)
            else:
                self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)
                
        db.close()
            
if __name__ == '__main__':
    unittest.main(verbosity=2)
    
    