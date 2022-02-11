import pickle

from utils import get_embedding

docs_path = './../data/docs.tsv'
sample_path = './../data/sample.csv'

with open('embeddings.pkz', 'rb') as _file:
    embeddings = pickle.load(_file)

doc_embeddings = dict()
with open(docs_path + '/' + 'docs', encoding='UTF-8', errors='replace') as file:
    for line in file:
        doc_id, *_ = line.strip().split()
        doc_embedding = get_embedding(' '.join(_), embeddings)
        doc_embeddings[int(doc_id)] = doc_embedding
        print(doc_id)

with open('./docs.embeddings', 'wb') as file:
    pickle.dump(doc_embeddings, file)
