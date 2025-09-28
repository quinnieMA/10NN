# -*- coding: utf-8 -*-
"""
Created on Sat Aug 23 14:03:07 2025

@author: Lenovo
Machine Learning Valuation Modeling
Core Objective
•	Predict M&A valuation multiples using financial and sentiment features
•	Compare multiple ML algorithms' performance
Data Structure
•	Target variables: 5 valuation multiples (Revenue, EBITDA, EBIT, PBT, PAT)
•	Financial features: 8 metrics (revenue, EBITDA, assets, equity, deal value, etc.)
•	Sentiment features: 8 text analysis metrics (positive/negative percentages from 4 dictionaries)
•	Total 16 predictive features
ML Methodology
•	Models compared:
o	SVM with RBF kernel
o	XGBoost (100 estimators)
o	K-Nearest Neighbors (k=5)
o	Neural Network (1 hidden layer, 50 neurons)
•	Evaluation metrics: RMSE and R² scores
•	Data preprocessing: StandardScaler normalization, 80/20 train-test split
Analysis Pipeline
1.	Data cleaning (remove missing values)
2.	Feature standardization
3.	Model training and validation
4.	Performance comparison across all multiples
5.	Feature importance analysis (XGBoost)
Visualization Outputs
•	Model performance comparison (R² scores bar charts)
•	Feature importance plots (color-coded: blue=financial, red=sentiment)
•	Results exported to Excel with detailed metrics
Key Features
•	Automated model comparison across 5 valuation multiples
•	Financial vs. sentiment feature importance analysis
•	Results automatically saved to designated directory
•	Comprehensive performance metrics for model selection
Application Value
•	Identifies best-performing models for each valuation multiple
•	Reveals which financial/sentiment factors drive M&A pricing
•	Provides predictive tools for deal valuation assessment

"""

import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVR
from xgboost import XGBRegressor
from sklearn.neighbors import KNeighborsRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt

# 1. 加载数据
df = pd.read_stata(r"d:/OneDrive/MA/acquisition/mergedtemp.dta")

# 2. 定义变量列表
# 2. Define Target Variables and Features
target_multipliers = [
    'pre_rev_mul_ly', 'pre_ebitda_mul_ly', 'pre_ebit_mul_ly', 
    'pre_pbt_mul_ly', 'pre_pat_mul_ly'
]

financial_features = [
    'pre_deal_tar_rev_rev_last_avail_', 'pre_deal_tar_ebitda_last_avail_y',
    'pre_deal_tar_ebit_last_avail_yr', 'pre_deal_tar_pbt_last_avail_yr',
    'pre_deal_tar_ta_last_avail_yr', 'pre_deal_tar_na_last_avail_yr',
    'pre_deal_tar_eq_last_avail_yr', 'deal_value'
]

sentiment_features = [
    'G_positive_percent', 'G_negative_percent',
    'H_positive_percent', 'H_negative_percent',
    'LM_positive_percent', 'LM_negative_percent',
    'NRC_positive_percent', 'NRC_negative_percent'
]

all_features = financial_features + sentiment_features


# 3. 数据预处理
def preprocess_data(df, target):
    """准备建模数据"""
    # 移除缺失值
    model_df = df[all_features + [target]].dropna()
    
    # 标准化特征
    scaler = StandardScaler()
    X = scaler.fit_transform(model_df[all_features])
    y = model_df[target].values
    
    return train_test_split(X, y, test_size=0.2, random_state=42)

# 4. 定义评估函数
def evaluate_model(model, X_test, y_test):
    preds = model.predict(X_test)
    return {
        'RMSE': np.sqrt(mean_squared_error(y_test, preds)),
        'R2': r2_score(y_test, preds)
    }

# 5. 建立和比较多个模型
results = {}
for target in target_multipliers:
    print(f"\n=== 正在分析 {target} ===")
    
    # 准备数据
    X_train, X_test, y_train, y_test = preprocess_data(df, target)
    
    # 初始化模型
    models = {
        'SVM': SVR(kernel='rbf', C=1.0),
        'XGBoost': XGBRegressor(n_estimators=100),
        'KNN': KNeighborsRegressor(n_neighbors=5),
        'Neural Network': MLPRegressor(hidden_layer_sizes=(50,), max_iter=1000)
    }
    
    # 训练和评估
    target_results = {}
    for name, model in models.items():
        model.fit(X_train, y_train)
        target_results[name] = evaluate_model(model, X_test, y_test)
        print(f"{name:15} R2: {target_results[name]['R2']:.3f}")
    
    results[target] = target_results

# 6. 可视化结果
plt.figure(figsize=(12, 8))
for i, target in enumerate(target_multipliers, 1):
    plt.subplot(2, 3, i)  # 调整为2行3列布局
    r2_scores = [results[target][m]['R2'] for m in models]
    plt.bar(models.keys(), r2_scores)
    plt.title(f"{target} 模型比较")
    plt.ylim(min(0, min(r2_scores)-0.1)), max(1, max(r2_scores)+0.1) # 动态调整y轴范围
    plt.ylabel('R2 Score')
    plt.xticks(rotation=45)  # x轴标签旋转
plt.tight_layout()

# 自动创建输出目录
output_dir = r"d:\OneDrive\MA\acquisition\results"
os.makedirs(output_dir, exist_ok=True)
plt.savefig(os.path.join(output_dir, "valuation_model_comparison.png"))
plt.show()

# 7. 特征重要性分析 (以XGBoost为例)
plt.figure(figsize=(15, 10))
for i, target in enumerate(target_multipliers, 1):
    model = XGBRegressor(n_estimators=100)
    X_train, _, y_train, _ = preprocess_data(df, target)  # 只用训练数据
    model.fit(X_train, y_train)
    
    plt.subplot(2, 3, i)
    # 按特征类型着色
    colors = ['skyblue' if feat in financial_features else 'salmon' for feat in all_features]
    pd.Series(model.feature_importances_, index=all_features
             ).sort_values().plot.barh(color=colors)
    plt.title(f"{target} 特征重要性")
    plt.xlabel('Importance Score')
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "feature_importance.png"))
plt.show()

# 8. 保存结果到Excel
results_df = pd.DataFrame.from_dict({
    (target, model): metrics 
    for target in results 
    for model, metrics in results[target].items()
}, orient='index')

results_df.to_excel(os.path.join(output_dir, "valuation_results.xlsx"))