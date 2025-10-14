import itertools

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset
from tqdm import tqdm

from models import AlphaNet


class MyDataset(Dataset):
    def __init__(self, X: np.ndarray, y: np.ndarray):
        self.X = torch.tensor(X).float()
        self.y = torch.tensor(y).float()

    def __len__(self):
        return len(self.X)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]

def load_model(model_path: str, device: str = 'cuda') -> AlphaNet:
    """Load a trained AlphaNet model."""
    # Model hyper-parameters
    parameters = {
        'window': 10,
        'stride': 10,
        'n_features': 8  # Adjust if the feature count differs
    }

    model = AlphaNet(d=parameters['window'],
                    stride=parameters['stride'],
                    n=parameters['n_features'])

    if device == 'cuda':
        model = model.to(device)
        model = nn.DataParallel(model)
    
    # Load weights
    model.load_state_dict(torch.load(model_path))
    model.eval()
    
    return model

def extract_features(model: AlphaNet, X: torch.Tensor) -> torch.Tensor:
    """Extract intermediate features before the FC stack."""
    features_fe, features_p, i = [], [], 0
    for extractor, batch_norm in zip(model.module.feature_extractors, model.module.batch_norms1):
        # Feature extraction + batch norm + flatten
        x = extractor(X)
        x = batch_norm(x)
        features_fe.append(x.flatten(start_dim=1))

        # Pooling + batch norm + flatten
        x_avg = model.module.batch_norms2[i](model.module.avg_pool(x))
        x_max = model.module.batch_norms2[i + 1](model.module.max_pool(x))
        x_min = model.module.batch_norms2[i + 2](-model.module.max_pool(-x))
        features_p.append(x_avg.flatten(start_dim=1))
        features_p.append(x_max.flatten(start_dim=1))
        features_p.append(x_min.flatten(start_dim=1))
        i += 3

    # Residual-style concatenation
    f1 = torch.cat(features_fe, dim=1)
    f2 = torch.cat(features_p, dim=1)
    features = torch.cat([f1, f2], dim=1)

    return features

def feature_importance_analysis(model: AlphaNet, dataloader: DataLoader, device: str = 'cuda') -> np.ndarray:
    """Gradient-based feature importance analysis."""
    model.eval()
    feature_importance = None
    total_samples = 0

    for X, y in tqdm(dataloader, desc="Computing gradient importance"):
        X = X.to(device)
        y = y.to(device)
        X.requires_grad = True
        # Extract features
        features = extract_features(model, X)

        # Forward through the fully connected layers
        connected_features = model.module.linear_layer(features)
        connected_features = model.module.relu(connected_features)
        connected_features = model.module.dropout(connected_features)
        y_pred = model.module.output_layer(connected_features)

        # Loss computation
        loss = nn.MSELoss()(y_pred.reshape(-1), y)
        features.retain_grad()
        # Backpropagate
        loss.backward()

        # Accumulate feature importance
        batch_importance = torch.abs(features.grad).mean(dim=0).cpu().numpy()[int(-features.shape[1]/2):]
        
        if feature_importance is None:
            feature_importance = batch_importance * len(X)
        else:
            feature_importance += batch_importance * len(X)
        
        total_samples += len(X)
    
    return feature_importance / total_samples

def map_feature_to_expression(feature_idx: int) -> str:
    """Map a feature index back to its symbolic expression."""
    # Feature name catalogue
    feature_names = ['close_adj', 'open', 'close', 'avg_price', 'high', 'low', 'Volume', 'Amount']
    extractor_names = ['ts_corr','ts_cov','ts_stddev','ts_zscore','ts_return','decaylinear','ts_mean']
    pool_operations = ['ts_mean', 'ts_max', 'ts_min']
    C_features=list(itertools.combinations (feature_names,2))
    combination1= list(itertools.product (extractor_names[:2],pool_operations,C_features))
    combination2 = list(itertools.product (extractor_names[2:],pool_operations,feature_names))
    all_combination=combination1+combination2
    combination = list(all_combination[feature_idx])
    operators = combination[:2]
    operators.reverse()
    features = [i for j in combination[2:] for i in (j if isinstance(j,tuple) else (j,))]
    expression = f'BN({operators[0]}(BN({operators[1]}({','.join(features)},10)),3))'

    return expression

def analyze_feature_importance(model_path: str, X: np.ndarray, y: np.ndarray, 
                             batch_size: int = 256, device: str = 'cuda') -> pd.DataFrame:
    """Run the full gradient-based feature importance pipeline."""
    # Build the dataloader
    dataset = MyDataset(X, y)
    dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=False)
    
    # Load the trained model
    model = load_model(model_path, device)
    
    # Compute importance scores
    gradient_importance = feature_importance_analysis(model, dataloader, device)
    
    # Assemble the results DataFrame
    results = []
    for i in range(len(gradient_importance)):
        feature_expr = map_feature_to_expression(i)
        results.append({
            'feature_index': i,
            'feature_expression': feature_expr,
            'importance': gradient_importance[i]
        })
    
    df = pd.DataFrame(results)
    df = df.sort_values('importance', ascending=False)
    
    return df

if __name__ == "__main__":
    # Example usage
    model_path = "Models/AlphaNet_batch_size1000.pt"  # Replace with the actual model path
    X = np.load('Features.npy')
    y = np.load('Labels.npy')
    
    # Analyse feature importance
    importance_df = analyze_feature_importance(model_path, X, y, batch_size=1000)

    # Report the top features
    print("\nTop 10 features:")
    print(importance_df.head(10))

    # Persist results to disk
    importance_df.to_csv('feature_importance.csv', index=False)
