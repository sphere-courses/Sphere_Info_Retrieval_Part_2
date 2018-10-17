import pickle

base_path = './../data/statistics/text_doclen.pkz'
with open(base_path, 'rb') as file:
    doclens = pickle.load(file)
    print(sum(doclens.values()))