import numpy as np
import pickle

embeddings = dict()

with open('./model.tsv', 'r', encoding='UTF-8', errors='replace') as file:
    for line in file:
        word, *_ = line.strip().split()
        embedding = np.array([float(value) for value in _])
        embeddings[word] = embedding

with open('./embeddings.pkz', 'wb') as file:
    pickle.dump(embeddings, file)
