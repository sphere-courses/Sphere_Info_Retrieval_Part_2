import pickle

with open('docs.embeddings', 'rb') as file:
    embeddings = pickle.load(file)


print(embeddings['привет'], embeddings['пока'])
