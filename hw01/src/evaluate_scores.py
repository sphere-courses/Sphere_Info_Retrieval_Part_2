import sys
import time
from itertools import combinations
from multiprocessing.pool import ThreadPool

import math
import pickle
from collections import defaultdict

from config import *


def get_idf(word: str):
    cf = float(text_cf_dict[word] + 1. * title_cf_dict[word]) + 1.
    df = float(text_df_dict[word] + 1. * title_df_dict[word]) + 1.

    idf_1 = -math.log(1 - math.exp(-1.5 * cf / N_DOCUMENTS)) + math.log(df / N_DOCUMENTS)
    # idf = math.log(1 - math.exp(-1.5 * cf / N_DOCUMENTS))
    idf_2 = -math.log(MEAN_DOCLEN * N_DOCUMENTS / cf)
    # idf = math.log(cf / N_DOCUMENTS)
    # idf = math.log(df / N_DOCUMENTS)

    return (idf_1 + idf_2) / 2


def get_score(query: str, doc_id: int, doc_base_path: str):
    # './../data/content/20170707_1/doc.2351.dat'
    text_tf_path = doc_base_path \
        .replace('content', 'statistics/tf').replace('/doc.', '_text_stem/doc.').replace('.dat', '.pkz')
    title_tf_path = doc_base_path \
        .replace('content', 'statistics/tf').replace('/doc.', '_title_stem/doc.').replace('.dat', '.pkz')

    text_pairs_path = doc_base_path \
        .replace('content', 'statistics/pairs').replace('/doc.', '_text_stem/doc.').replace('.dat', '.pkz')
    title_pairs_path = doc_base_path \
        .replace('content', 'statistics/pairs').replace('/doc.', '_title_stem/doc.').replace('.dat', '.pkz')

    text_hop_pairs_path = doc_base_path \
        .replace('content', 'statistics/hop_pairs').replace('/doc.', '_text_stem/doc.').replace('.dat', '.pkz')
    title_hop_pairs_path = doc_base_path \
        .replace('content', 'statistics/hop_pairs').replace('/doc.', '_title_stem/doc.').replace('.dat', '.pkz')

    score = 0.
    with open(text_tf_path, 'rb') as text_tf_file, \
            open(title_tf_path, 'rb') as title_tf_file, \
            open(text_pairs_path, 'rb') as text_pairs_file, \
            open(title_pairs_path, 'rb') as title_pairs_file, \
            open(text_hop_pairs_path, 'rb') as text_hop_pairs_file, \
            open(title_hop_pairs_path, 'rb') as title_hop_pairs_file:
        text_tf_dict = pickle.load(text_tf_file)
        title_tf_dict = pickle.load(title_tf_file)

        text_pairs_dict = pickle.load(text_pairs_file)
        title_pairs_dict = pickle.load(title_pairs_file)

        text_hop_pairs_dict = pickle.load(text_hop_pairs_file)
        title_hop_pairs_dict = pickle.load(title_hop_pairs_file)

        words = query.split(' ')
        W_single = 0.
        W_pair = 0.
        W_all = 0.

        doclen = float(text_doclen_dict[doc_id] + title_doclen_dict[doc_id])
        N_missed = 0
        sum_log_p = 0.
        for word in words:
            tf = float(text_tf_dict[word] + 1. * title_tf_dict[word])
            hdr = float(title_tf_dict[word])

            IDF = get_idf(word)

            TF_1 = tf / (tf + K1 + K2 * doclen)
            # TF_2 = hdr / (1. + hdr)
            TF_2 = 1. if hdr > 0 else 0.

            W_single += IDF * (TF_1 + 1.5 * TF_2)

            sum_log_p += IDF
            if tf < 1.:
                N_missed += 1

        for word_1, word_2 in combinations(words, 2):
            IDF_1 = get_idf(word_1)
            IDF_2 = get_idf(word_2)

            pair_1_2 = float(text_pairs_dict[(word_1, word_2)] + 1. * title_pairs_dict[(word_1, word_2)])
            hop_pair_1_2 = float(text_hop_pairs_dict[(word_1, word_2)] + 1. * title_hop_pairs_dict[(word_1, word_2)])
            pair_2_1 = float(text_pairs_dict[(word_2, word_1)] + 1. * title_pairs_dict[(word_2, word_1)])

            TF = (W_P * pair_1_2 + W_S * hop_pair_1_2 + W_I * pair_2_1)
            W_pair += (IDF_1 + IDF_2) * TF / (1. + TF)

        W_all = sum_log_p * (0.03 ** N_missed)

        score = W_single + W_PAIR * W_pair + W_ALL * W_all

    return score


n_p_path = './../data/numerate.path.pkz'

target_docs_path = './../data/sample.technosphere.ir2.textrelevance.sabmission.txt'

text_doclen_path = './../data/statistics/text_doclen.pkz'
title_doclen_path = './../data/statistics/title_doclen.pkz'
text_cf_path = './../data/statistics/text_cf_count.pkz'
title_cf_path = './../data/statistics/title_cf_count.pkz'
text_df_path = './../data/statistics/text_df_count.pkz'
title_df_path = './../data/statistics/title_df_count.pkz'

with open(text_cf_path, 'rb') as text_cf_file, \
        open(title_cf_path, 'rb') as title_cf_file, \
        open(text_df_path, 'rb') as text_df_file, \
        open(title_df_path, 'rb') as title_df_file, \
        open(text_doclen_path, 'rb') as text_doclen_file, \
        open(title_doclen_path, 'rb') as title_doclen_file:
    text_cf_dict = pickle.load(text_cf_file)
    title_cf_dict = pickle.load(title_cf_file)
    text_df_dict = pickle.load(text_df_file)
    title_df_dict = pickle.load(title_df_file)
    text_doclen_dict = pickle.load(text_doclen_file)
    title_doclen_dict = pickle.load(title_doclen_file)


def process(part_idx):
    queries_path = './../data/queries.numerate_processed_chunks/chunk_{0}.txt'.format(part_idx)
    submition_path = './../data/queries.numerate_processed_chunks/results/chunk_{0}.txt'.format(part_idx)

    # queries_path = './../data/queries.numerate_processed.txt'
    # submition_path = './../data/submition{0}.txt'.format(str(time.time()))

    # query number to query text
    queries = defaultdict(tuple)
    with open(queries_path, 'r', encoding='UTF-8', errors='ignore') as file:
        for line in file:
            idx, query_main, query_syn = (part.strip() for part in line.split('\t'))
            queries[int(idx)] = (query_main, query_syn)

    with open(n_p_path, 'rb') as file:
        n_p_dict = pickle.load(file)

    result = defaultdict(list)
    queries_processed = 0.
    with open(target_docs_path, 'r', encoding='UTF-8', errors='ignore') as file:
        file.readline()
        for line in file:
            query_id, doc_id = (int(val) for val in line.strip().split(','))
            if query_id not in queries:
                continue
            try:
                query_main, query_syn = queries[query_id]
                score_main = get_score(query_main, doc_id, n_p_dict[doc_id])
                score_syn = get_score(query_syn, doc_id, n_p_dict[doc_id])
                score = score_main + 0.8 * score_syn
                print(doc_id, score_main, score_syn)
            except KeyError:
                score = 0.
            result[query_id].append((doc_id, score))
            print(queries_processed)
            queries_processed += 1

    for key in result.keys():
        result[key] = sorted(result[key], key=lambda val: val[1])

    with open(submition_path, 'w') as file:
        file.write('QueryId,DocumentId\n')
        for key in sorted(result.keys()):
            for doc_id, _ in result[key]:
                file.write(str(key) + ',' + str(doc_id) + '\n')


pool = ThreadPool(8)
pool.map(process, list(range(1, 9)))
pool.close()
pool.join()
