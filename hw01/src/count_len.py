import pickle
from utils import *
from collections import defaultdict

base_path = './../data/content/'
p_n_path = './../data/path.numerate.pkz'
text_doclen_path = './../data/statistics/text_doclen.pkz'
title_doclen_path = './../data/statistics/title_doclen.pkz'

with open(p_n_path, 'rb') as p_n_file:
    path_to_num = pickle.load(p_n_file)
    num_to_text_len = defaultdict(int)
    num_to_title_len = defaultdict(int)

    cnt = 0
    for path in get_text_stem_files(base_path):
        print(cnt)
        cnt += 1

        base = get_base_file_path(path)
        with open(path, 'r', encoding='UTF-8', errors='ignore') as file:
            num_to_text_len[int(path_to_num[base])] = len(file.read().strip().split(' '))

    with open(text_doclen_path, 'wb') as text_doclen_file:
        pickle.dump(num_to_text_len, text_doclen_file)

    cnt = 0
    for path in get_title_stem_files(base_path):
        print(cnt)
        cnt += 1

        base = get_base_file_path(path)
        with open(path, 'r', encoding='UTF-8', errors='ignore') as file:
            num_to_title_len[int(path_to_num[base])] = len(file.read().strip().split(' '))

    with open(title_doclen_path, 'wb') as title_doclen_file:
        pickle.dump(num_to_title_len, title_doclen_file)
