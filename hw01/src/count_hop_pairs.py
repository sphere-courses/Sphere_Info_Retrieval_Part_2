import pickle
from utils import *
from collections import defaultdict


def count_hop_pairs(file_generator):
    cnt = 0
    for file_path in file_generator:
        print(cnt)
        cnt += 1
        hop_pairs_dict = defaultdict(int)
        with open(file_path, 'r', encoding='UTF-8', errors='ignore') as file:
            text = file.read().strip()
            words = text.split(' ')
            for idx in range(len(words) - 2):
                hop_pairs_dict[(words[idx], words[idx + 2])] += 1

        hop_pairs_path = file_path_to_hop_pairs_path(file_path)
        os.makedirs(os.path.dirname(hop_pairs_path), exist_ok=True)
        with open(hop_pairs_path, 'wb') as pairs_file:
            pickle.dump(hop_pairs_dict, pairs_file)


base_path = './../data/content/'
base_result_path = './../data/statistics/'

count_hop_pairs(get_text_stem_files(base_path))
count_hop_pairs(get_title_stem_files(base_path))
