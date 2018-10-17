import os
import pickle
from utils import *
from collections import defaultdict


def count_cf(file_generator, cf_path):
    cf_dict = defaultdict(int)

    cnt = 0
    for file_path in file_generator:
        print(cnt)
        cnt += 1
        with open(file_path, 'r', encoding='UTF-8', errors='ignore') as file:
            text = file.read().strip()
            words = text.split(' ')
            for word in words:
                cf_dict[word] += 1
    with open(cf_path, 'wb') as cf_file:
        pickle.dump(cf_dict, cf_file)


base_path = './../data/content/'
base_result_path = './../data/statistics/'

text_cf_path = base_result_path + 'text_cf_count.pkz'
title_cf_path = base_result_path + 'title_cf_count.pkz'

count_cf(get_text_stem_files(base_path), text_cf_path)
count_cf(get_title_stem_files(base_path), title_cf_path)
