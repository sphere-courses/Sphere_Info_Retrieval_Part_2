import numpy as np
from xgboost import XGBRegressor
from utils_trees import make_predictions_xgboost, Data


def calc_dcg(_y_predict, _y):
    pred_idxs = np.argsort(_y_predict)[::-1]
    pred_yy = _y[pred_idxs]
    return np.sum((np.power(2., pred_yy) - 1.) / np.log(np.arange(1, _y.shape[0] + 1) + 1.))


def calc_ndcg(_y_predict, _y):
    return calc_dcg(_y_predict, _y) / calc_maxdcg(_y)


def calc_maxdcg(_y, _t=None):
    if _t is None:
        _t = _y.shape[0]
    _y_sorted = np.sort(_y[:_t])[::-1]
    return np.sum((np.power(2., _y_sorted[:_t]) - 1.) / np.log(np.arange(1, _t + 1) + 1))


def calc_dz(_y_predict, _y, _t=None):
    if _t is None:
        _t = _y.shape[0]
    p = np.argsort(_y_predict)[::-1] + 1.
    _numerator = np.power(2., _y)
    _denumerator = (np.arange(1, _y.shape[0] + 1) <= _t) / np.log(p + 1)
    _numerator_diff = _numerator[:, None] - _numerator[None, :]
    _denumerator_diff = _denumerator[:, None] - _denumerator[None, :]
    max_dcg = calc_maxdcg(_y, _t)
    if np.isclose(max_dcg, 0.):
        return 0.
    return np.abs(_numerator_diff * _denumerator_diff) / max_dcg


def calc_grad(_y_predict, _y, dz):
    sign = np.sign(_y[:, None] - _y[None, :])
    _sij = np.abs(_y_predict[:, None] - _y_predict[None, :])
    return np.sum(-sign * dz / (1 + np.exp(_sij)), axis=1)


def calc_hess(_y_predict, _y, dz):
    _sij = np.abs(_y_predict[:, None] - _y_predict[None, :])
    hessian = np.sum(dz / (1 + np.exp(_sij)) / (1 + np.exp(-_sij)), axis=1)
    hessian[np.isclose(hessian, 0.)] = 1.
    return hessian


val = 0


def objective(dataset, _dataset_test):
    def calc_objective(_y, _y_predict):
        global val
        val += 1
        print(val)
        print("!!", calc_ndcg(_y_predict, _y))
        grad = np.empty([_y.shape[0]], dtype=np.float32)
        hess = np.empty([_y.shape[0]], dtype=np.float32)
        for idx in range(dataset.unique_query_indices.shape[0]):
            indexes = np.array(dataset.query_document_indices[idx])
            dz = calc_dz(_y_predict[indexes], _y[indexes])
            (grad[indexes], hess[indexes]) = (calc_grad(_y_predict[indexes], _y[indexes], dz),
                                              calc_hess(_y_predict[indexes], _y[indexes], dz))
        return grad, hess

    return calc_objective


dataset_train = Data().load('./data/train.txt')
dataset_test = Data().load('./data/test.txt')
print("DATA LOADED")

params = {'objective': objective(dataset_train, dataset_test), 'max_depth': 8,
          'nthread': 8, 'n_estimators': 1500, 'learning_rate': 0.3}
model = XGBRegressor(**params)
print("CREATED")

model.fit(dataset_train.x, dataset_train.y)
print("FITTED")

make_predictions_xgboost(model, dataset_test)
