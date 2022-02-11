import pickle

from scipy.spatial.distance import cosine

from utils import get_embedding

embeddings_path = './embeddings.pkz'
docs_embeddings_path = './docs.embeddings'

queries_path = './../data/queries_fixed.tsv'
sample_path = './../data/sample.csv'
result_path = './../data/submit.csv'

# load embeddings
with open(embeddings_path, 'rb') as file:
    embeddings = pickle.load(file)

with open(docs_embeddings_path, 'rb') as file:
    docs_embeddings = pickle.load(file)

# load queries
queries = dict()
with open(queries_path, 'r', encoding='UTF-8', errors='replace') as file:
    for line in file:
        query_id, query_text = line.strip().split('\t')
        queries[int(query_id)] = query_text

prev_query_id = 0
prev_query_docs = []
with open(sample_path, 'r', encoding='UTF-8', errors='replace') as file:
    with open(result_path, 'w', encoding='UTF-8', errors='replace') as result_file:
        header = file.readline()
        result_file.write(header)
        for line in file:
            query_id, doc_id = [int(value) for value in line.strip().split(',')]
            if query_id == prev_query_id:
                prev_query_docs.append(doc_id)
            else:
                query_embedding = get_embedding(queries[prev_query_id].upper(), embeddings)
                distances = []
                for doc_id in prev_query_docs:
                    doc_embedding = docs_embeddings[doc_id]
                    distances.append((doc_id, cosine(query_embedding, doc_embedding)))
                for doc_id, _ in sorted(distances, key=lambda obj: obj[1]):
                    result_file.write(str(prev_query_id) + ',' + str(doc_id) + '\n')
                print(prev_query_id)
                prev_query_id = query_id
                prev_query_docs = []
