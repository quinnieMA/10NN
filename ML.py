# -*- coding: utf-8 -*-
"""
Created on Wed Jul  9 14:32:00 2025

@author: FM
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.svm import SVR
from xgboost import XGBRegressor
from sklearn.neighbors import KNeighborsRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm
import warnings
import matplotlib
from sklearn.pipeline import make_pipeline

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']  # Windows系统
plt.rcParams['axes.unicode_minus'] = False
warnings.filterwarnings('ignore')

# 1. 加载数据
df = pd.read_stata(r"D:/OneDrive/MA/acquisition/mergedtemp.dta")

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

# 3. 改进的数据预处理
def preprocess_data(df, target, features):
    """更稳健的数据预处理"""
    # 选择变量并移除缺失值
    model_df = df[features + [target]].dropna()
    
    # 检查样本量
    if len(model_df) < 100:
        return None, None, None, None
    
    # 分离特征和标签
    X = model_df[features].values
    y = model_df[target].values.ravel()  # 确保标签是一维数组
    
    # 使用RobustScaler处理异常值
    scaler = RobustScaler()
    X = scaler.fit_transform(X)
    
    return train_test_split(X, y, test_size=0.2, random_state=42)

# 4. 优化的模型配置 (保持不变)
models = {
    'RandomForest': {
        'model': RandomForestRegressor(random_state=42),
        'params': {
            'n_estimators': [100, 200],
            'max_depth': [5, 10, None],
            'min_samples_split': [2, 5]
        }
    },
    'XGBoost': {
        'model': XGBRegressor(random_state=42),
        'params': {
            'n_estimators': [100, 200],
            'learning_rate': [0.01, 0.1],
            'max_depth': [3, 5],
            'subsample': [0.8, 1.0]
        }
    },
    'SVM': {
        'model': SVR(),
        'params': {
            'C': [0.1, 1, 10],
            'kernel': ['linear', 'rbf'],
            'gamma': ['scale', 'auto']
        }
    },
    'NeuralNetwork': {
        'model': MLPRegressor(random_state=42),
        'params': {
            'hidden_layer_sizes': [(50,), (100,), (50, 50)],
            'alpha': [0.0001, 0.001],
            'learning_rate_init': [0.001, 0.01],
            'max_iter': [2000]  # 增加迭代次数
        }
    }
}

# 5. 分析流程 (修改目标变量和特征)
results = pd.DataFrame()
feature_importances = pd.DataFrame()

for target in tqdm(target_multipliers, desc="处理估值乘数"):
    # 准备数据
    X_train, X_test, y_train, y_test = preprocess_data(df, target, all_features)
    if X_train is None:
        print(f"\n跳过 {target} - 样本不足")
        continue
    
    # 训练和评估每个模型
    for model_name, model_info in models.items():
        try:
            # 使用管道整合预处理和模型
            pipe = make_pipeline(
                RobustScaler(),
                GridSearchCV(model_info['model'], model_info['params'], 
                           cv=3, scoring='r2', n_jobs=-1)
            )
            
            pipe.fit(X_train, y_train)
            best_model = pipe.named_steps['gridsearchcv'].best_estimator_
            
            # 评估
            preds = pipe.predict(X_test)
            metrics = {
                '估值乘数': target,
                '模型': model_name,
                'R2': r2_score(y_test, preds),
                'RMSE': np.sqrt(mean_squared_error(y_test, preds)),
                '最佳参数': str(pipe.named_steps['gridsearchcv'].best_params_)
            }
            
            results = pd.concat([results, pd.DataFrame([metrics])], ignore_index=True)
            
            # 特征重要性
            if hasattr(best_model, 'feature_importances_'):
                importances = best_model.feature_importances_
            elif hasattr(best_model, 'coef_'):
                importances = np.abs(best_model.coef_[0] if len(best_model.coef_.shape) > 1 else np.abs(best_model.coef_))
            else:
                importances = np.zeros(len(all_features))
                
            temp_df = pd.DataFrame({
                '特征': all_features,
                '重要性': importances,
                '估值乘数': target,
                '模型': model_name
            })
            feature_importances = pd.concat([feature_importances, temp_df], ignore_index=True)
            
            print(f"{model_name:15} R2: {metrics['R2']:.3f}")
            
        except Exception as e:
            print(f"\n{model_name} 处理 {target} 时出错: {str(e)}")
            continue

# 6. 保存结果
output_path = r"D:/OneDrive/MA/acquisition/results/"
results.to_excel(f"{output_path}valuation_model_results.xlsx", index=False)
feature_importances.to_excel(f"{output_path}valuation_feature_importances.xlsx", index=False)

# 7. 可视化改进
def save_plot(fig, filename):
    try:
        fig.savefig(f"{output_path}{filename}", dpi=300, bbox_inches='tight')
        plt.close()
    except Exception as e:
        print(f"保存 {filename} 时出错: {str(e)}")

# 模型性能比较
plt.figure(figsize=(12, 6))
sns.barplot(data=results, x='模型', y='R2', hue='估值乘数')
plt.title('估值乘数预测模型性能比较')
plt.ylim(min(0, results['R2'].min() - 0.1), 1)
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
save_plot(plt.gcf(), "valuation_model_performance.png")

# 特征重要性可视化
for target in target_multipliers:
    for model in ['XGBoost', 'RandomForest']:
        plt.figure(figsize=(10, 6))
        temp_df = feature_importances[
            (feature_importances['估值乘数'] == target) &
            (feature_importances['模型'] == model)
        ].sort_values('重要性', ascending=False).head(10)
        
        if len(temp_df) > 0:
            sns.barplot(data=temp_df, x='重要性', y='特征')
            plt.title(f'{target} - {model} 特征重要性 Top 10')
            save_plot(plt.gcf(), f"{target}_{model}_feature_importance.png")