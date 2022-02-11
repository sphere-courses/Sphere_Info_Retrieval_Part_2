import numpy as np


class DocReader:
    def __init__(self, base_path, quantity=7000, prefix='docs_part_', docs=()):
        print(docs)
        self.docs = np.unique(list(sorted(docs)))
        self.quantity = quantity
        self.base_path = base_path
        self.prefix = prefix
        self.ptr = 0
        self.file = None
        self.files = dict()
        self.file_n, self.line_n = None, None

    def __iter__(self):
        return self

    def __next__(self):
        if self.ptr == len(self.docs):
            for file in self.files:
                file.close()
            raise StopIteration
        else:
            file_n = self.docs[self.ptr] // self.quantity
            line_n = self.docs[self.ptr] % self.quantity
            if self.file is None or file_n > self.file_n:
                if file_n in self.files:
                    self.file = self.files[file_n]
                else:
                    self.file = open(self.base_path + '/' + self.prefix + '{0:02d}'.format(file_n))
                    self.files[file_n] = self.file
                self.file_n = file_n
                self.line_n = 0
            line = None
            while self.line_n <= line_n:
                line = self.file.readline().strip()
                self.line_n += 1
            self.ptr += 1
            return line
