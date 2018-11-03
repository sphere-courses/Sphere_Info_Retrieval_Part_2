import os
import time

import pickle
import numpy as np
from collections import defaultdict

import torch
from torch.utils.data import Dataset


use_cuda = False
device = None
t_type = torch.float32


class HW02Dataset(Dataset):
    def __init__(self, train=True):
        # data_path = "./data/train.txt" if train else "./data/test.txt"
        data_path = "./data/train_vsmall.txt" if train else "./data/train_vsmall.txt"
        pkz_path = "./data/train_vsmall.pkz" if train else "./data/train_vsmall.pkz"
        # pkz_path = "./data/train.pkz" if train else "./data/test.pkz"
        self.train = train
        if os.path.exists(pkz_path):
            with open(pkz_path, "rb") as _file:
                pkz_dataset = pickle.load(_file)
            self.queries_to_idx = pkz_dataset.queries_to_idx
            self.documents = torch.tensor(pkz_dataset.documents, dtype=t_type, device=device)
            self.marks = pkz_dataset.marks
            return
        # n_lines = 473134 if train else 236743
        n_lines = 3 if train else 3
        n_fea = 699

        # queries_to_idx[q] contains indexes of documents corresponding query q
        self.queries_to_idx = defaultdict(list)

        self.documents = np.zeros([n_lines, n_fea], dtype=np.float32)
        self.marks = np.zeros([n_lines, 1], dtype=np.float32)

        with open(data_path, "r") as data_file:
            for idx, line in enumerate(data_file):
                if idx != 0 and idx % 50000 == 0:
                    print(idx)
                line = line.strip()
                mark, queiry_id, *document = line.split(" ")
                self.queries_to_idx[int(queiry_id.split(':')[1]) - 1].append(idx)
                self.marks[idx] = mark
                for pos_val in document:
                    pos, val = pos_val.split(':')
                    self.documents[idx, int(pos) - 1] = float(val)
        with open(pkz_path, 'wb') as dump_file:
            pickle.dump(self, dump_file)

    def __len__(self):
        return len(self.queries_to_idx)

    def __getitem__(self, _idx):
        if self.train:
            return self.documents[self.queries_to_idx[_idx]], self.marks[self.queries_to_idx[_idx]]
        return self.documents[self.queries_to_idx[_idx + 19944]], self.marks[self.queries_to_idx[_idx + 19944]]


def make_predictions_torch(_net):
    prediction_file = open('submition_' + str(time.time()) + '.txt', 'w')
    prediction_file.write('QueryId,DocumentId\n')
    dataset_test = HW02Dataset(False)
    doc_idx = 1
    for _idx in range(len(dataset_test)):
        _x, _ = dataset_test[_idx]
        y_pred = _net(_x)
        idx_sorted = np.argsort(y_pred.cpu().detach().numpy().reshape(-1))[::-1]
        for _jdx in range(idx_sorted.shape[0]):
            prediction_file.write(str(19945 + _idx) + ',' + str(doc_idx + idx_sorted[_jdx]) + '\n')
        doc_idx += idx_sorted.shape[0]
