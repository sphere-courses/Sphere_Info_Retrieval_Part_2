import re

import nltk
import pyaspeller
from pymystem3 import Mystem
from langdetect import detect
from string import punctuation


def process_query(query):
    if len(query) == 0:
        return query
    if detect(query) in change_lang:
        query = ''.join(change_rule[c] if c in change_rule.keys() else c for c in query)

    # filter useless characters
    query = ''.join([c if c in en_symbols or c in ru_symbols or c in digits else ' ' for c in query])
    # convert to lower case
    words = [word.lower().strip() for word in query.split(' ')]
    query = ' '.join(words)
    # stem query
    lemmas = stemmer.lemmatize(query)
    # filter useless characters
    query = ' '.join(word for word in lemmas if len(word) > 0 and word != '\n')
    query = re.sub(' +', ' ', query)
    return query


# queries_path = './../data/queries.numerate.txt'
# queries_processed_path = './../data/queries.numerate_processed.txt'

queries_path = './../data/queries.numerate_processed.txt'
queries_processed_path = './../data/queries.numerate_processed_stem.txt'

change_lang = ('da', 'sq', 'cy')
change_rule = {
    'q': 'й', 'w': 'ц', 'e': 'у', 'r': 'к', 't': 'е', 'y': 'н', 'u': 'г', 'i': 'ш',
    'o': 'щ', 'p': 'з', '[': 'х', ']': 'ъ', 'a': 'ф', 's': 'ы', 'd': 'в', 'f': 'а',
    'g': 'п', 'h': 'р', 'j': 'о', 'k': 'л', 'l': 'д', ';': 'ж', '\'': 'э', 'z': 'я',
    'x': 'ч', 'c': 'с', 'v': 'м', 'b': 'и', 'n': 'т', 'm': 'ь', ',': 'б', '.': 'ю'
    }

digits = set('1234567890')
en_symbols = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
ru_symbols = set('йцукенгшщзхъэждлорпавыфячсмитьбюЙЦУКЕНГШЩЗХЪЭЖДЛОРПАВЫФЯЧСМИТЬБЮ')

stemmer = Mystem()
speller = pyaspeller.YandexSpeller(find_repeat_words=True, ignore_capitalization=True)

with open(queries_path, 'r', encoding='UTF-8') as queries_file, \
        open(queries_processed_path, 'w', encoding='UTF-8') as queries_processed_file:
    for line in queries_file:
        idx, query_main, query_syn = (part.strip() for part in line.split('\t'))
        query_main, query_syn = process_query(query_main), process_query(query_syn)
        print(query_main, '!!', query_syn)
        queries_processed_file.write(str(idx) + '\t' + query_main + '\t' + query_syn + '\n')
