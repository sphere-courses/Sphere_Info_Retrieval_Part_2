import numpy as np
from pyaspeller import YandexSpeller


_en_symbols = 'qazxswedcvfrtgbnhyujmkiolpQAZXSWEDCVFRTGBNHYUJMKILOP'
_ru_symbols = 'йфячыцувсмакепитрнгоьблшщдюжзэхъЙФЯЧЫЦУВСМАКЕПИТРНГОЬБЛШЩДЮЖЗЭХЪ'

_eng_chars = u"~!@#$%^&qwertyuiop[]asdfghjkl;'zxcvbnm,./QWERTYUIOP{}ASDFGHJKL:\"|ZXCVBNM<>?"
_rus_chars = u"ё!\"№;%:?йцукенгшщзхъфывапролджэячсмитьбю.ЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭ/ЯЧСМИТЬБЮ,"
_trans_table = dict(zip(_eng_chars, _rus_chars))


def fix_layout(s):
    return u''.join([_trans_table.get(c, c) for c in s])


def is_broken_layout(line):
    n_en_symbols = sum(c in _en_symbols for c in line)
    n_ru_symbols = sum(c in _ru_symbols for c in line)
    true_exceptions = [27, 740, 1200, 1207, 4175]    # need to change layout
    false_exceptions = [60]  # have to save current layout
    number = int(line[:line.find('\t')])
    if number in true_exceptions:
        return True
    if number in false_exceptions:
        return False
    return n_ru_symbols < n_en_symbols and n_ru_symbols < 6


def fix_queries(path):
    speller = YandexSpeller(ignore_capitalization=True)

    with open(path, 'r', encoding='UTF-8', errors='replace') as file:
        new_path = path[:path.rfind('.')] + '_fixed' + path[path.rfind('.'):]
        with open(new_path, 'w') as result_file:
            for line in file:
                # fix layout
                if is_broken_layout(line):
                    line = fix_layout(line)

                # fix typos
                changes = {change['word']: change['s'][0] for change in speller.spell(line) if len(change['s']) > 0}
                for word, suggestion in changes.items():
                    line = line.replace(word, suggestion)
                result_file.write(line)


def to_starspace_format(path):
    with open(path, 'r', encoding='UTF-8', errors='replace') as file:
        new_path = path + '_ss'
        with open(new_path, 'w') as result_file:
            for line in file:
                doc = line.strip().lower().split('\t')

                print(doc[0])
                result_file.write('\t'.join(doc[1:]) + '\n')


def get_embedding(line, embeddings, embedding_len=100):
    embedding = np.zeros([embedding_len], dtype=np.float)
    for word in line.split():
        if word in embeddings:
            embedding += embeddings[word]
    return embedding
