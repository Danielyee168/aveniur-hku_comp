# AlphaNet: an explainable deep neural network for alpha mining

## Introduction

## Data Preprocess

## Feature description:

The original features fed for our AlphaNet is no difference from our former project. They are just normal end of day data, which consist of 8 columns: open, close, high, low, adjusted close, vwap, trade amount, trade volume. Meanwhile, we use the same time range and stock pool with our former project

## Feature to image:

Inspired by the structure of convolutional neural networks (CNNs), AlphaNet organizes Eod data into a "data image" format for input into the network. The format for our feature image is shown in figure 1. Each day, for every stock, we set a rolling window (30 in our report) and collect all the Eod data during this window, finally we can get a feature image of size 8*30 for an individual stock. We first expand this feature image to cross-section and move our rolling window by 5 days, then we repeat this procedure throughout the time horizon. Finally, our X would be the shape of , for example, if we have 5000 stocks and 1000 trading days, then we can collect X with shape 1000000*8*30. For labels, we use stock return in the next five days, which is consistent with our moving step.

## Model Structure

## Conclusion

AlphaNet presents a novel and interpretable deep learning framework for alpha factor mining by integrating convolutional neural networks with genetic algorithms. Through structured data preprocessing, custom feature extraction layers, and a unique loss function based on the Information Coefficient, the model effectively identifies and combines predictive factors. Extensive backtesting demonstrates AlphaNet’s ability to generate stable and robust alpha across training and testing periods. Its interpretability, achieved through gradient analysis and symbolic expression matching, sets it apart from traditional black-box models. AlphaNet offers a promising direction for combining machine learning with financial domain knowledge in quantitative investing.

Reference

AlphaNet: A Neural Network for Factor Mining, Huatai Security
