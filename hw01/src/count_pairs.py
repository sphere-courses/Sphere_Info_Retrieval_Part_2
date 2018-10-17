import pickle
from utils import *
from collections import defaultdict


def count_pairs(file_generator):
    cnt = 0
    for file_path in file_generator:
        print(cnt)
        cnt += 1
        pairs_dict = defaultdict(int)
        with open(file_path, 'r', encoding='UTF-8', errors='ignore') as file:
            text = file.read().strip()
            words = text.split(' ')
            for idx in range(len(words) - 1):
                pairs_dict[(words[idx], words[idx + 1])] += 1

        pairs_path = file_path_to_pairs_path(file_path)
        os.makedirs(os.path.dirname(pairs_path), exist_ok=True)
        with open(pairs_path, 'wb') as pairs_file:
            pickle.dump(pairs_dict, pairs_file)


base_path = './../data/content/'
base_result_path = './../data/statistics/'

count_pairs(get_text_stem_files(base_path))
count_pairs(get_title_stem_files(base_path))
