import gc
import pickle
import numpy as np
import pandas as pd
from audtorch.metrics.functional import pearsonr
from torch.utils.tensorboard import SummaryWriter
from tqdm import tqdm

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, Dataset
from models import AlphaNet
use_gpu = True
if use_gpu:
    from gpu_tools import *
    os.environ["CUDA_VISIBLE_DEVICES"] = ','.join([str(obj) for obj in select_gpu(query_gpu())])
    device = 'cuda'
    print("Using GPU.")
else:
    device = 'cpu'
    print("Using CPU.")
tb = SummaryWriter('tensorboard/runs/AlphaNet')
seed = 123456789
np.random.seed(seed)
torch.manual_seed(seed)

class MyDataset(Dataset):

    def __init__(self, X:np.array, y:np.array):
        super(MyDataset, self).__init__()
        self.X = torch.tensor(X).float()
        self.y = torch.tensor(y).float()

    def __len__(self):
        return len(self.X)

    def __getitem__(self, idx:int):
        return self.X[idx], self.y[idx]

def save_model(model:nn.Module, name:str):
    torch.save(model.state_dict(), name)



def compute_loss(predicted_r, gt_r, eps=1e-8):
      pred = predicted_r.reshape(-1) - predicted_r.reshape(-1).mean()
      target = gt_r.reshape(-1) - gt_r.reshape(-1).mean()
      num = (pred * target).mean()
      denom = pred.pow(2).mean().sqrt() * target.pow(2).mean().sqrt()
      corr = num / (denom + eps)
      corr = torch.clamp(corr, min=-1 + eps, max=1 - eps)
      return -corr.abs()


def load_model(model:nn.Module, name:str):
    weights = torch.load(name)
    model.load_state_dict(weights)



def train_loop(dataloader, model, loss_fn, opt, epoch):
    model.train()
    running_loss = 0.0
    current = 0

    with tqdm(dataloader) as t:
        for batch, (X, y) in enumerate(t):
            X = X.to(device)
            y = y.to(device)
            y_pred = model(X)
            loss = loss_fn(y_pred.reshape(-1), y)
            if not torch.isfinite(loss):
                print("Non-finite loss detected", loss.item())
                print("pred nan:", torch.isnan(y_pred).any().item(), "target nan:", torch.isnan(y).any().item())
                break
            opt.zero_grad()
            loss.backward()
            opt.step()

            running_loss = (len(X) * loss.item() + running_loss * current) / (len(X) + current)
            current += len(X)
            t.set_postfix({'running_loss': running_loss})
            tb.add_scalars(f'Training Loss',
                           {'Training': running_loss},
                           batch + len(dataloader) * epoch)
    name, param = next(model.named_parameters())
    if param.grad is not None and not torch.isfinite(param.grad).all():
        print(f'Non-finite grads detected in {name}')

    return running_loss


def val_loop(dataloader, model, loss_fn, epoch):
    model.eval()
    running_loss = 0.0
    current = 0

    with torch.no_grad():
        with tqdm(dataloader) as t:
            for batch, (X, y) in enumerate(t):
                X = X.to(device)
                y = y.to(device)
                y_pred = model(X)
                loss = loss_fn(y_pred.reshape(-1), y)

                running_loss = (len(X) * loss.item() + running_loss * current) / (len(X) + current)
                current += len(X)
                t.set_postfix({'running_loss': running_loss})
                tb.add_scalars(f'Validation Loss',
                           {'Validation': running_loss},
                           batch + len(dataloader) * epoch)
    return running_loss

def generate_dataset(X_:np.array,Y_:np.array,train_ind:pd.Index,val_ind:pd.Index,test_ind:pd.Index,size:int):
    train_set = MyDataset(X_[train_ind], Y_[train_ind])
    valid_set = MyDataset(X_[val_ind],
                          Y_[val_ind])
    test_set = MyDataset(X_[test_ind], Y_[test_ind])

    # Create data loaders
    train_dl = DataLoader(train_set, batch_size=size, shuffle=False)
    valid_dl= DataLoader(valid_set, batch_size=size, shuffle=False)
    test_dl = DataLoader(test_set, batch_size=size, shuffle=False)
    return train_dl, valid_dl, test_dl

X = np.load('Features.npy')
Y = np.load('Labels.npy')
indices = np.load('indices.npy',allow_pickle=True)
indices = pd.DataFrame(indices, columns=['st_code', 'trade_date'])
indices = indices[~indices.st_code.map(lambda x: x.endswith('BJ'))]  # Exclude instruments ending with BJ
sorted_indices = indices.sort_values(by=['trade_date', 'st_code'])
parameters = {
    'n_epoch': int(50),
    'batch_size': int(1000),
    'lr': float(0.001),
    'window': int(10),
    'stride': int(10),
    'n_features': int(X.shape[1]),
    'early_stopping_epoch': int(10),
}
print('Shape of X: ', X.shape)
print('Shape of Y: ', Y.shape)
train_index = sorted_indices[sorted_indices.trade_date < '2024-11-25 00:00:00'].index
val_index = sorted_indices[
    (sorted_indices.trade_date >= '2024-12-01 00:00:00') & (sorted_indices.trade_date < '2025-03-31 00:00:00')].index
test_index = sorted_indices[sorted_indices.trade_date > '2025-04-05 00:00:00'].index

train_loader, valid_loader, test_loader = generate_dataset(X, Y, train_index, val_index, test_index,
                                                           size=parameters['batch_size'])

bets_model_set = None
# Candidate model variants: 'AlphaNet', 'alphanet_att', 'AlphaNet_fe'
model_name = 'AlphaNet'
net = AlphaNet(d=parameters['window'], stride=parameters['stride'], n=parameters['n_features'])
if use_gpu:
    net = net.to(device)
    net = nn.DataParallel(net)


features = pd.DataFrame(index=sorted_indices[['trade_date', 'st_code']],data=X[sorted_indices.index,0,29])
r = pd.DataFrame(index=sorted_indices[['trade_date', 'st_code']],data=Y[sorted_indices.index])
features['st_code'],r['st_code'] = features.index.map(lambda x: x[1]),r.index.map(lambda x: x[1])
features['trade_date'],r['trade_date'] = features.index.map(lambda x: x[0]),r.index.map(lambda x: x[0])
features_df = features.pivot(index='trade_date', columns='st_code', values=0)
r_df = r.pivot(index='trade_date', columns='st_code', values=0)
optimizer = optim.Adam(net.parameters(), lr=parameters['lr'])
if __name__ == "__main__":

    last_min_ind = 0
    min_val_loss = 1e9
    for t in range(parameters['n_epoch']):
        print(f"Epoch {t}\n-------------------------------")
        train_loss = train_loop(train_loader, net, compute_loss, optimizer, t)
        val_loss = val_loop(valid_loader, net, compute_loss, t)
        print('Validation loss: ', val_loss)

        # Log epoch-level losses
        tb.add_scalars('Loss/Epoch',
                      {'Training': train_loss,
                       'Validation': val_loss},
                      t)

        if val_loss < min_val_loss:
            last_min_ind = t
            min_val_loss = val_loss
            save_model(net, name = ('Models/' + model_name  + f'_batch_size{parameters["batch_size"]}'+ '.pt') )

        elif t - last_min_ind >= parameters['early_stopping_epoch']:
            break

    print('Done!')
    print('Best epoch: {}, val_loss: {}'.format(last_min_ind, min_val_loss))
