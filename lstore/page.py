class Page:

    __slots__ = 'num_records', 'data', 'dirty'

    def __init__(self, dirty=False):
        """
        First 4088 bytes -> data (511 eight bytes int)
        Next 2 bytes -> num_records
        Last 5 -> Not Used
        """
        self.num_records = 0
        self.data = bytearray(4096)
        self.dirty = dirty

    """
    Debug Only
    """

    def __str__(self):
        string = 'Current size: {}/511 records\n'.format(self.num_records)
        if self.dirty:
            string += 'DIRTY!\n'
        string += '=' * 5 + '\n'
        if self.num_records > 0:
            for i in range(self.num_records):
                string += '{}: {}\n'.format(i, self.get(i))
        else:
            string += 'Empty\n'
        return string

    def __repr__(self):
        return self.__str__()

    def has_capacity(self):
        return self.num_records < 511

    def write(self, value):
        assert self.num_records < 511
        self.data[self.num_records * 8 : self.num_records * 8 + 8] = \
        value.to_bytes(8, 'big', signed=True)
        self.num_records += 1
        self.dirty = True # page is dirty

    def get(self, rec_num):
        assert 0 <= rec_num < 511
        return int.from_bytes(self.data[rec_num * 8 : rec_num * 8 + 8], 'big', signed=True)

    def set(self, rec_num, value):
        assert 0 <= rec_num < 511
        self.data[rec_num * 8 : rec_num * 8 + 8] = value.to_bytes(8, 'big', signed=True)
        self.dirty = True # page is dirty

    def get_and_set(self, rec_num, value):
        assert 0 <= rec_num < 511
        old = int.from_bytes(self.data[rec_num * 8 : rec_num * 8 + 8], 'big', signed=True)
        self.data[rec_num * 8 : rec_num * 8 + 8] = value.to_bytes(8, 'big', signed=True)
        self.dirty = True # page is dirty
        return old
    
    def close(self):
        self.data[4088 : 4090] = self.num_records.to_bytes(2, 'big', signed=False)

    def open(self, data):
        self.data = data
        self.num_records = int.from_bytes(data[4088 : 4090], 'big', signed=False)
