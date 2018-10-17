import platform
import re
import os
import pickle
import multiprocessing
from io import StringIO
from multiprocessing import freeze_support

from lxml import etree
from html.parser import HTMLParser

from pymystem3 import Mystem


digits = set('1234567890')
en_symbols = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
ru_symbols = set('йцукенгшщзхъэждлорпавыфячсмитьбюЙЦУКЕНГШЩЗХЪЭЖДЛОРПАВЫФЯЧСМИТЬБЮ')


def accept_part(part):
    en_cnt = 0
    ru_cnt = 0
    di_cnt = 0
    for c in part:
        if c in en_symbols:
            en_cnt += 1
        elif c in ru_symbols:
            ru_cnt += 1
        elif c in digits:
            di_cnt += 1
    if en_cnt + ru_cnt == 0:
        return False
    en_proba = float(en_cnt) / float(en_cnt + ru_cnt)
    if en_proba < 0.95 or en_cnt == len(part) - ru_cnt - part.count(' '):
        return True
    return False


class TextHTMLParser(HTMLParser):
    cyrillic_codes = (
        'be', 'bg', 'os', 'mk', 'ru', 'sr', 'tg',
        'uk', 'cu', 'mn', 'av', 'ce', 'ab', 'ba',
        'ky', 'tt', 'cv', 'kk', 'kv', 'ro'
    )
    text_tags = (
        'tt','i','b','big','small','em','strong','dfn','code',
        'samp','kbd','var','cite','abbr','acronym','sub','sup',
        'span','bdo','address','div','a','object','p',
        'h1','h2', 'h3', 'h4', 'h5', 'h6',
        'pre','q','ins','del','dt','dd','li','label','option',
        'textarea','fieldset','legend','button','caption','td','th','script',
        'style'
    )
    paragraph_tags = (
        'p', 'br', 'div'
    )

    def __init__(self):
        HTMLParser.__init__(self)
        self._text = []
        self._title = []
        self._in_title = False

    def handle_data(self, text):
        text = text.strip()
        if len(text) > 0:
            if accept_part(text) or self._in_title:
                # print("+++++", text)
                pass
            else:
                # print("-----", text)
                return
            if self._in_title:
                self._title.append(text)
            else:
                self._text.append(text)

    def handle_starttag(self, tag, attrs):
        if tag == 'title':
            self._in_title = True

    def handle_endtag(self, tag):
        if tag == 'title':
            self._in_title = False

    def text(self):
        text = ' '.join(self._text)
        # remove all \r
        text = re.sub('[ \r]+', ' ', text)
        # remove translations
        text = text.replace("-\n", "")
        # remove useless characters
        text = re.sub('\W+', ' ', text)
        # split digits
        text = ' '.join(re.split('(\d+)', text))
        text = ' '.join((word.lower() for word in text.split(' ') if word != ''))
        return text

    def title(self):
        text = ' '.join(self._title)
        # remove all \r
        text = re.sub('[ \r]+', ' ', text)
        # remove translations
        text = text.replace("-\n", "")
        # remove useless characters
        text = re.sub('\W+', ' ', text)
        # split digits
        text = ' '.join(re.split('(\d+)', text))
        text = ' '.join((word.lower() for word in text.split(' ') if word != ''))
        return text


def get_text_and_title(path, *args):
    with open(path, 'r', encoding='UTF-8', errors='ignore') as file:
        text = file.read()

        # fix DOM tree using lxml tools
        parser = etree.HTMLParser()
        tree = etree.parse(StringIO(text), parser)
        text = str(etree.tostring(tree.getroot(), pretty_print=True, method="html"), encoding='UTF-8')

        # parse fixed html and get title, text, meta, headers, ...
        parser = TextHTMLParser()
        parser.feed(text)
        return parser.text(), parser.title()


stemmer = Mystem()


def get_text_and_title_stem(text_path, title_path):
    print(text_path)
    with open(text_path, 'r', encoding='UTF-8', errors='ignore') as text_file, \
            open(title_path, 'r', encoding='UTF-8', errors='ignore') as title_file:
        text = text_file.read()
        title = title_file.read()

    text_words, title_words = stemmer.lemmatize(text), stemmer.lemmatize(title)

    text = ' '.join(word for word in text_words if len(word) > 0 and word != '\n')
    title = ' '.join(word for word in title_words if len(word) > 0 and word != '\n')
    text = re.sub(' +', ' ', text)
    title = re.sub(' +', ' ', title)
    return text, title


def prepare_dir(base_path, step_suffix, dir_path, preparer):
    prepared_files = 0

    try:
        os.mkdir(base_path + dir_path + step_suffix[0])
    except FileExistsError:
        pass
    try:
        os.mkdir(base_path + dir_path.replace('text', 'title') + step_suffix[1])
    except FileExistsError:
        pass

    file_paths = [f for f in os.listdir(base_path + dir_path)]
    for file_path in file_paths:
        real_path = base_path + dir_path + '/' + file_path
        text_real_path_result = base_path + dir_path + step_suffix[0] + '/' + file_path
        title_real_path_result = base_path + dir_path.replace('text', 'title') + step_suffix[1] + '/' + file_path

        # if platform.system() == 'Windows' and os.path.getsize(real_path) < 800 * 1024:
        #     continue
        # if platform.system() != 'Windows' and os.path.getsize(real_path) > 800 * 1024:
        #     continue
        if os.path.exists(text_real_path_result) and os.path.exists(title_real_path_result):
            continue

        text, title = preparer(real_path, real_path.replace('text', 'title'))

        with open(text_real_path_result, 'w', encoding='UTF-8', errors='ignore') as result_file:
            result_file.write(text)
        with open(title_real_path_result, 'w', encoding='UTF-8', errors='ignore') as result_file:
            result_file.write(title)
        prepared_files += 1


# __name__ = '__text_and_title__'
if __name__ == '__text_and_title__':
    base_path = './../data/content/'
    dir_paths = [f for f in os.listdir(base_path) if f.count('_') == 1]
    step_suffix = ['_text', '_title']
    print(dir_paths)

    jobs = []
    for dir_path in dir_paths:
        # get_text fot each document
        process = multiprocessing.Process(target=prepare_dir, args=(base_path, step_suffix, dir_path, get_text_and_title))
        jobs.append(process)
        process.start()

# __name__ = '__stem_text_and_title__'
if __name__ == '__stem_text_and_title__':
    base_path = './../data/content/'
    dir_paths = [f for f in os.listdir(base_path) if f.count('_') == 2 and f.find('text') >= 0]
    step_suffix = 2 * ['_stem']
    print(dir_paths, len(dir_paths))

    jobs = []
    for dir_path in dir_paths:
        # get_text fot each document
        process = multiprocessing.Process(target=prepare_dir, args=(base_path, step_suffix, dir_path, get_text_and_title_stem))
        jobs.append(process)
        process.start()


if __name__ == '__idx_to_path__':
    base_path = './../data/content/'
    url_path = './../data/urls.numerate.txt'
    num_path = './../data/path.numerate.txt'
    dir_paths = [f for f in os.listdir(base_path) if f.count('_') == 1]

    print(dir_paths)

    url_to_num = dict()
    with open(url_path, 'r', encoding='UTF-8', errors='ignore') as url_file:
        for line in url_file:
            num, url = line.strip().split('\t')
            url_to_num[url] = num

    cnt = 0
    with open(num_path, 'a', encoding='UTF-8', errors='ignore') as num_file:
        for dir_path in dir_paths:
            file_paths = [f for f in os.listdir(base_path + dir_path)]
            for file_path in file_paths:
                full_file_path = base_path + dir_path + '/' + file_path
                with open(full_file_path, 'r', encoding='UTF-8', errors='ignore') as file:
                    url = file.readline().strip()
                    num_file.write(full_file_path + '\t' + str(url_to_num[url]) + '\n')
                    cnt += 1


if __name__ == '__idx_path_to_dict__':
    num_path = './../data/path.numerate.txt'
    p_n_path = './../data/path.numerate.pkz'
    n_p_path = './../data/numerate.path.pkz'
    path_numerate = dict()
    numerate_path = dict()
    with open(num_path, 'r', encoding='UTF-8', errors='ignore') as num_file, \
            open(p_n_path, 'wb') as p_n_file, open(n_p_path, 'wb') as n_p_file:
        for line in num_file:
            path, num = line.strip().split('\t')
            path_numerate[path] = int(num)
            numerate_path[int(num)] = path
            print(line)
        pickle.dump(path_numerate, p_n_file)
        pickle.dump(numerate_path, n_p_file)
