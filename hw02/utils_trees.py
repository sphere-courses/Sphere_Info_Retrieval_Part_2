import time

import numpy as np
from sklearn.datasets import load_svmlight_file


class Data:
    def __init__(self):
        self.x = None
        self.y = None
        self.queries = None
        self.query_document_indices = None
        self.unique_query_indices = None

    def load(self, filename):
        self.x, self.y, documents_query = load_svmlight_file(filename, query_id=True)

        self.queries = []
        self.query_document_indices = []
        self.unique_query_indices = np.unique(documents_query)
        for query_id in self.unique_query_indices:
            self.query_document_indices.append(np.where(documents_query == query_id)[0])
            if query_id % 1000 == 0:
                print(query_id)
        return self


def make_predictions_xgboost(model, dataset_test):
    prediction_file = open('submition_' + str(time.time()) + '.txt', 'w')
    prediction_file.write('QueryId,DocumentId\n')
    doc_idx = 1
    for _idx in range(len(dataset_test.unique_query_indices)):
        _x = dataset_test.x[dataset_test.query_document_indices[_idx]]
        y_pred = model.predict(_x)
        idx_sorted = np.argsort(y_pred.reshape(-1))[::-1]
        for _jdx in range(idx_sorted.shape[0]):
            prediction_file.write(str(dataset_test.unique_query_indices[_idx]) + ',' + str(doc_idx + idx_sorted[_jdx]) + '\n')
        doc_idx += idx_sorted.shape[0]
