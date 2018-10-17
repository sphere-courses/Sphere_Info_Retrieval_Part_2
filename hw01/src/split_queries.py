queries_path = './../data/queries.numerate_processed_stem.txt'
base_path = './../data/queries.numerate_processed_chunks/'
n_splits = 8
split_size = 50

split_files = [open(base_path + 'chunk_{}'.format(str(idx)) + '.txt', 'w', encoding='UTF-8', errors='ignore') for idx in range(1, n_splits + 1)]

with open(queries_path, 'r', encoding='UTF-8', errors='ignore') as queries_file:
    for line in queries_file:
        idx, query_main, query_syn = (part.strip() for part in line.split('\t'))
        print(int(idx))
        split_files[int(idx) // split_size].write(idx + '\t' + query_main + '\t' + query_syn + '\n')
