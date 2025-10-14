import torch
import torch.nn as nn
from audtorch.metrics.functional import pearsonr


# -------------------- Feature extraction layers --------------------

class ts_corr(nn.Module):
    """
    Compute the correlation coefficient between two sequences over the past d periods.
    """

    def __init__(self, d=10, stride=10):
        """
        d: Window length in periods.
        stride: Step size along the temporal dimension.
        """
        super(ts_corr, self).__init__()
        self.d = d
        self.stride = stride
        
    def forward(self, X):
        # n: number of features, T: length of the time window
        batch_size, n, T = X.shape

        # Initialise the output tensor
        w = int((T - self.d) / self.stride + 1)
        h = int(n * (n - 1) / 2)
        Z = torch.zeros(batch_size, h, w, device=X.device)

        # Vectorised computation replaces explicit loops
        eps = 1e-8

        for i in range(w):
            start = i * self.stride
            end = start + self.d
            
            # Extract the current temporal slice
            window = X[:, :, start:end]  # [batch_size, n, d]
            start_idx = 0
            # Compute correlation for every feature pair
            for j in range(n - 1):
                # Current feature
                x = window[:, j:j+1, :]  # [batch_size, 1, d]
                # Remaining features
                y = window[:, j+1:, :]   # [batch_size, n-j-1, d]
                
                # Correlation components
                x_mean = x.mean(dim=2, keepdim=True)
                y_mean = y.mean(dim=2, keepdim=True)
                
                x_std = torch.std(x, dim=2, keepdim=True)
                y_std = torch.std(y, dim=2, keepdim=True)
                
                # Covariance
                cov = ((x - x_mean) * (y - y_mean)).mean(dim=2)
                
                # Correlation coefficient
                denom = (x_std * y_std).squeeze(-1)
                corr = cov / (denom + eps)
                corr = torch.nan_to_num(corr, nan=0.0, posinf=0.0, neginf=0.0)
                
                # Populate the feature map

                end_idx = start_idx + (n - j - 1)
                Z[:, start_idx:end_idx, i] = corr
                start_idx = end_idx
        return Z


class ts_cov(nn.Module):
    """
    Compute covariance between two sequences over the past d periods.
    """

    def __init__(self, d=10, stride=10,):
        super(ts_cov, self).__init__()
        self.d = d
        self.stride = stride
        
    def forward(self, X):
        batch_size, n, T = X.shape
        w = int((T - self.d) / self.stride + 1)
        h = int(n * (n - 1) / 2)
        Z = torch.zeros(batch_size, h, w, device=X.device)

        for i in range(w):
            start = i * self.stride
            end = start + self.d
            
            # Extract the current temporal slice
            window = X[:, :, start:end]  # [batch_size, n, d]
            start_idx = 0
            for j in range(n - 1):
                # Current feature
                x = window[:, j:j+1, :]  # [batch_size, 1, d]
                # Remaining features
                y = window[:, j+1:, :]   # [batch_size, n-j-1, d]
                
                # Means
                x_mean = x.mean(dim=2, keepdim=True)
                y_mean = y.mean(dim=2, keepdim=True)
                
                # Covariance
                cov = ((x - x_mean) * (y - y_mean)).mean(dim=2)
                
                # Populate the feature map
                end_idx = start_idx + (n - j - 1)
                Z[:, start_idx:end_idx, i] = cov
                start_idx = end_idx

        return Z


class ts_return(nn.Module):
    """
    Compute simple returns over the past d periods.
    """

    def __init__(self, d=10, stride=10,):
        """
        d: Window length in periods.
        stride: Step size along the temporal dimension.
        """
        super(ts_return, self).__init__()
        self.d = d
        self.stride = stride
        
    def forward(self, X):
        # n: number of features, T: window length
        batch_size, n, T = X.shape

        # Initialise the output tensor
        w = int((T - self.d) / self.stride + 1)
        Z = torch.zeros(batch_size, n, w, device=X.device)

        # Vectorised return calculation
        eps = 1e-8

        for i in range(w):
            start = i * self.stride
            end = start + self.d
            window = X[:, :, start:end]  # [batch_size, n, d]
            
            # Compute returns for this slice
            first = window[:, :, 0]
            denom = torch.where(first.abs() < eps, torch.full_like(first, eps), first)
            return_d = (window[:, :, -1] - first) / denom
            return_d = torch.nan_to_num(return_d, nan=0.0, posinf=0.0, neginf=0.0)
            
            # Populate the feature map
            Z[:, :, i] = return_d

        return Z


class ts_stddev(nn.Module):
    """
    Compute standard deviations over the past d periods.
    """

    def __init__(self, d=10, stride=10,):
        super(ts_stddev, self).__init__()
        self.d = d
        self.stride = stride
        
    def forward(self, X):
        batch_size, n, T = X.shape
        w = int((T - self.d) / self.stride + 1)
        Z = torch.zeros(batch_size, n, w, device=X.device)

        # Vectorised standard deviation
        for i in range(w):
            start = i * self.stride
            end = start + self.d
            window = X[:, :, start:end]  # [batch_size, n, d]
            
            # Standard deviation
            std = torch.std(window, dim=2)
            
            # Populate the feature map
            Z[:, :, i] = std

        return Z


class ts_zscore(nn.Module):
    """
    Compute z-scores over the past d periods.
    """

    def __init__(self, d=10, stride=10,):
        super(ts_zscore, self).__init__()
        self.d = d
        self.stride = stride
        
    def forward(self, X):
        batch_size, n, T = X.shape
        w = int((T - self.d) / self.stride + 1)
        Z = torch.zeros(batch_size, n, w, device=X.device)

        # Vectorised z-score computation
        for i in range(w):
            start = i * self.stride
            end = start + self.d
            window = X[:, :, start:end]  # [batch_size, n, d]
            
            # Mean and standard deviation
            mean = torch.mean(window, dim=2)
            std = torch.std(window, dim=2)
            
            # Z-score
            z_score = mean / (std + 1e-8)  # Add epsilon to avoid division by zero
            
            # Populate the feature map
            Z[:, :, i] = z_score

        return Z


class ts_decaylinear(nn.Module):
    """
    Compute linearly decayed averages over the past d periods.
    """

    def __init__(self, d=10, stride=10,):
        super(ts_decaylinear, self).__init__()
        self.d = d
        self.stride = stride
        
    def forward(self, X):
        batch_size, n, T = X.shape
        w = int((T - self.d) / self.stride + 1)
        Z = torch.zeros(batch_size, n, w, device=X.device)

        # Construct linear weights
        weights = torch.arange(1, self.d + 1, device=X.device)
        normalized_w = weights / torch.sum(weights)

        # Vectorised weighted average
        for i in range(w):
            start = i * self.stride
            end = start + self.d
            window = X[:, :, start:end]  # [batch_size, n, d]
            
            # Weighted mean
            weighted_avg = torch.matmul(window, normalized_w)
            
            # Populate the feature map
            Z[:, :, i] = weighted_avg

        return Z


# -------------------- AlphaNet v1 --------------------

class AlphaNet(nn.Module):
    '''
    AlphaNet v1: input -> feature extractors -> pooling -> flatten/residual -> FC layers -> output.
    '''

    def __init__(self, d=10, stride=10, d_pool=3, s_pool=3, n=9):
        super(AlphaNet, self).__init__()

        # d: lookback window, stride: temporal stride
        self.d = d
        self.stride = stride
        
        # Feature dimension for ts_corr/ts_cov
        h = int(n * (n - 1) / 2)

        # Feature extractor modules
        self.feature_extractors = nn.ModuleList([
            ts_corr(self.d, self.stride),
            ts_cov(self.d, self.stride),
            ts_stddev(self.d, self.stride),
            ts_zscore(self.d, self.stride),
            ts_return(self.d, self.stride),
            ts_decaylinear(self.d, self.stride),
            nn.AvgPool1d(self.d, self.stride),
        ])

        # Batch norms applied after each extractor
        self.batch_norms1 = nn.ModuleList([
            nn.BatchNorm1d(h),
            nn.BatchNorm1d(h),
            nn.BatchNorm1d(n),
            nn.BatchNorm1d(n),
            nn.BatchNorm1d(n),
            nn.BatchNorm1d(n),
            nn.BatchNorm1d(n)
        ])

        # Pooling layers (parameter free)
        self.avg_pool = nn.AvgPool1d(d_pool, s_pool)
        self.max_pool = nn.MaxPool1d(d_pool, s_pool)

        # Batch norms after pooling outputs
        self.batch_norms2 = nn.ModuleList([])
        for _ in range(2):
            for _ in range(3):
                self.batch_norms2.append(nn.BatchNorm1d(h))
        for _ in range(5):
            for _ in range(3):
                self.batch_norms2.append(nn.BatchNorm1d(n))

        # Dimensionality after flattening all branches
        n_in = 2 * (h * 2 * 3 + n * 5 * 3)

        # Fully connected stack
        self.relu = nn.ReLU()
        self.dropout = nn.Dropout(p=0.5)
        self.linear_layer = nn.Linear(n_in, 30)
        self.output_layer = nn.Linear(30, 1)



    def forward(self, X):

        features_fe, features_p, i = [], [], 0
        for extractor, batch_norm in zip(self.feature_extractors, self.batch_norms1):
            # Feature extraction + batch norm + flatten
            x = extractor(X)
            x = batch_norm(x)
            features_fe.append(x.flatten(start_dim=1))

            # Pooling + batch norm + flatten
            x_avg = self.batch_norms2[i](self.avg_pool(x))
            x_max = self.batch_norms2[i + 1](self.max_pool(x))
            x_min = self.batch_norms2[i + 2](-self.max_pool(-x))
            features_p.append(x_avg.flatten(start_dim=1))
            features_p.append(x_max.flatten(start_dim=1))
            features_p.append(x_min.flatten(start_dim=1))
            i += 3

        # Residual-style concatenation
        f1 = torch.cat(features_fe, dim=1)
        f2 = torch.cat(features_p, dim=1)
        features = torch.cat([f1, f2], dim=1)

        # Fully connected layers and output
        features = self.linear_layer(features)
        features = self.relu(features)
        features = self.dropout(features)
        output = self.output_layer(features)

        return output
