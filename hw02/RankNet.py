import time

import torch
import torch.nn as nn
from torch.utils.data import RandomSampler

from itertools import permutations

from utils_torch import HW02Dataset, make_predictions_torch

use_cuda = False
device = None
t_type = torch.float32


def evaluate_loss(_y_predict, _y):
    _loss = torch.tensor([0.], dtype=t_type, device=device)
    for _idx, _jdx in permutations(range(_y_predict.shape[0]), 2):
        s_ij = _y_predict[_idx] - _y_predict[_jdx]
        if _y[_idx] > _y[_jdx]:
            _loss += torch.log(1. + torch.exp(-s_ij))
    return _loss


def evaluate_loss_mse(_y_predict, _y):
    alpha = torch.tensor([0.8], dtype=t_type, device=device)
    if y.shape[0] < 5:
        return torch.norm(_y_predict - torch.tensor(_y, dtype=t_type, device=device)) ** 2

    _loss = (torch.norm(_y_predict[:5] - torch.tensor(_y[:5], dtype=t_type, device=device)) ** 2 +
             alpha * torch.norm(_y_predict[5:] - torch.tensor(_y[5:], dtype=t_type, device=device)) ** 2)
    return _loss


dataset_train = HW02Dataset(True)
dataloader_train = RandomSampler(dataset_train)
net = nn.Sequential(
    nn.Linear(699, 500),
    nn.ReLU(),
    nn.Linear(500, 300),
    nn.ReLU(),
    nn.Linear(300, 150),
    nn.ReLU(),
    nn.Linear(150, 1),
).to(device=device).float()

optimizer = torch.optim.Adam(net.parameters(), lr=5e-3)

batch_size = 64
try:
    for n_iter in range(10):
        for idx in range(len(dataloader_train)):
            loss = torch.tensor([0.], dtype=t_type, device=device)
            loss_mse = torch.tensor([0.], dtype=t_type, device=device)
            for jdx, query_idx in enumerate(dataloader_train):
                if jdx == batch_size:
                    break
                x, y = dataset_train[query_idx]
                x.requires_grad = True
                y_predict = net(x)

                if y.shape[0] >= 2:
                    loss += evaluate_loss(y_predict, y)
                # mix pairwise loss and pointwise loss
                # 15 * max(mse loss) ~ max(pairwise loss)
                loss += 15. * evaluate_loss_mse(y_predict, y)
            print(idx, loss.data[0].cpu().numpy())
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
except KeyboardInterrupt:
    pass
with open('checkpoint_' + str(time.time()) + '.pkz', 'wb') as file:
    torch.save(net.state_dict(), file)

make_predictions_torch(net)
