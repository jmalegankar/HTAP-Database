class Page:

    __slots__ = 'num_records', 'data', 'dirty'

    def __init__(self, dirty=False):
        """
        First 4088 bytes -> data (511 eight bytes int)
        Next 2 bytes -> num_records
        Last 5 -> Not Used
        """
        self.data = bytearray(4096)
        self.dirty = dirty

    """
    Debug Only
    """

    def __str__(self):
        string = 'Physical Page\n'
        if self.dirty:
            string += 'DIRTY!\n'
        for i in range(511):
            string += '{}: {}\n'.format(i, self.get(i))
        return string

    def __repr__(self):
        return self.__str__()

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
        pass

    def open(self, data):
        self.data = data
