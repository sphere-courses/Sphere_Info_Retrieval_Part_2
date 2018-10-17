import os
import pickle
from utils import *
from collections import defaultdict


def count_df(file_generator, df_path):
    df_dict = defaultdict(int)

    cnt = 0
    for file_path in file_generator:
        print(cnt)
        cnt += 1

        tmp_dict = defaultdict(int)
        with open(file_path, 'r', encoding='UTF-8', errors='ignore') as file:
            text = file.read().strip()
            words = text.split(' ')
            for word in words:
                tmp_dict[word] = 1
        for key in tmp_dict.keys():
            df_dict[key] += 1
    with open(df_path, 'wb') as df_file:
        pickle.dump(df_dict, df_file)


base_path = './../data/content/'
base_result_path = './../data/statistics/'

text_df_path = base_result_path + 'text_df_count.pkz'
title_df_path = base_result_path + 'title_df_count.pkz'

count_df(get_text_stem_files(base_path), text_df_path)
count_df(get_title_stem_files(base_path), title_df_path)
