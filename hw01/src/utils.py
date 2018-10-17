import os


def get_text_stem_files(base_path):
    files = []
    text_dir_paths = [f for f in os.listdir(base_path) if f.count('_') == 3 and f.find('text_stem') >= 0]

    for dir_path in text_dir_paths:
        for file_path in os.listdir(base_path + dir_path):
            yield base_path + dir_path + '/' + file_path


def get_title_stem_files(base_path):
    files = []
    text_dir_paths = [f for f in os.listdir(base_path) if f.count('_') == 3 and f.find('title_stem') >= 0]

    for dir_path in text_dir_paths:
        for file_path in os.listdir(base_path + dir_path):
            yield base_path + dir_path + '/' + file_path


def get_base_file_path(path):
    if path.find('text'):
        path = path.replace('_text', '')
    if path.find('title'):
        path = path.replace('_title', '')
    if path.find('stem'):
        path = path.replace('_stem', '')
    return path


def file_path_to_tf_path(path):
    return path.replace('content', 'statistics/tf').replace('.dat', '.pkz')


def file_path_to_pairs_path(path):
    return path.replace('content', 'statistics/pairs').replace('.dat', '.pkz')


def file_path_to_hop_pairs_path(path):
    return path.replace('content', 'statistics/hop_pairs').replace('.dat', '.pkz')
