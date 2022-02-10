class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    """
    Debug Only
    """

    def __str__(self):
        string = 'Current size: {}/512 records\n'.format(self.num_records)
        string += '=' * 5 + '\n'
        if self.num_records > 0:
            for i in range(self.num_records):
                string += '{}: {}\n'.format(i, self.get(i))
        else:
            string += 'Empty\n'
        return string

    def has_capacity(self):
        return self.num_records < 512

    def write(self, value):
        self.data[self.num_records * 8 : self.num_records * 8 + 8] = \
        value.to_bytes(8, 'big', signed=True)
        self.num_records += 1

    def get(self, rec_num):
        return int.from_bytes(self.data[rec_num * 8 : rec_num * 8 + 8], "big", signed=True)

    def set(self, rec_num, value):
        self.data[rec_num * 8 : rec_num * 8 + 8] = value.to_bytes(8, 'big', signed=True)

    def get_and_set(self, rec_num, value):
        old = int.from_bytes(self.data[rec_num * 8 : rec_num * 8 + 8], "big", signed=True)
        self.data[rec_num * 8 : rec_num * 8 + 8] = value.to_bytes(8, 'big', signed=True)
        return old
