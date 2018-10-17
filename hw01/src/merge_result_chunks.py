import os
import time

from natsort import natsorted


base_path = './../data/queries.numerate_processed_chunks/results/'
submition_path = base_path + 'submition_{0}.txt'.format(str(time.time()))

paths = natsorted([f for f in os.listdir(base_path) if f.find('chunk') >= 0])

with open(submition_path, 'w') as submition_file:
    submition_file.write('QueryId,DocumentId\n')
    for path in paths:
        with open(base_path + path, 'r') as file:
            file.readline()
            for line in file:
                submition_file.write(line)
