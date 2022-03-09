import os
from lstore.table import Table
import lstore.bufferpool as bufferpool
import lstore.merge_worker as merge_worker

class Database():

    __slots__ = ('tables', 'path', 'merge_worker')

    def __init__(self):
        bufferpool.shared.start()
        self.tables = {}
        self.merge_worker = merge_worker.MergeWorker()

    # Not required for milestone1
    def open(self, path):
        bufferpool.shared.db_path(path) 
        
        if bufferpool.shared.db_exists():
            data = bufferpool.shared.read_metadata('database.db')
            if data is not None:
                for table in data:
                    self.create_table(table, None, None, True)

    def close(self):
        for table in self.tables.values():
            table.close()


        try:
            while True:
                self.merge_worker.queue.get_nowait()
                self.merge_worker.queue.task_done()
        except:
            pass

        bufferpool.shared.close()

        bufferpool.shared.write_metadata('database.db', (
            list(self.tables.keys())
        ))


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index, open_from_db=False):
        if name in self.tables:
            return self.tables[name]

        table = Table(name, num_columns, key_index, open_from_db, self.merge_worker)
        self.tables[name] = table

        bufferpool.shared.create_folder(name)

        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        del self.tables[name]

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        if name in self.tables:
            return self.tables[name]
        return None
