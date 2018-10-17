import pickle
from utils import *
from collections import defaultdict


def count_tf(file_generator):
    cnt = 0
    for file_path in file_generator:
        print(cnt)
        cnt += 1
        tf_dict = defaultdict(int)
        with open(file_path, 'r', encoding='UTF-8', errors='ignore') as file:
            text = file.read().strip()
            words = text.split(' ')
            for word in words:
                tf_dict[word] += 1

        tf_path = file_path_to_tf_path(file_path)
        os.makedirs(os.path.dirname(tf_path), exist_ok=True)
        with open(tf_path, 'wb') as tf_file:
            pickle.dump(tf_dict, tf_file)


base_path = './../data/content/'
base_result_path = './../data/statistics/'

count_tf(get_text_stem_files(base_path))
count_tf(get_title_stem_files(base_path))
