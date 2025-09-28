# -*- coding: utf-8 -*-
"""
Created on Sat Jul 26 20:17:23 2025

@author: FM
"""

# -*- coding: utf-8 -*-
"""
国家间并购倍数比较分析 - 修正版
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import f_oneway
import statsmodels.api as sm
from statsmodels.formula.api import ols
import os
from tqdm import tqdm

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# 1. 定义国家代码变量（根据您的数据结构修正）
country_vars = ['tar_country_code', 'acq_country_code']  # 使用主代码列

# 2. 定义保存图像函数
def save_plot(fig, filename):
    """保存图像到指定路径"""
    output_dir = r"C:\Users\FM\OneDrive\MA\acquisition\results"
    os.makedirs(output_dir, exist_ok=True)
    try:
        fig.savefig(os.path.join(output_dir, filename), 
                   dpi=300, bbox_inches='tight')
        plt.close()
    except Exception as e:
        print(f"保存 {filename} 时出错: {str(e)}")

# 3. 改进的国家比较分析函数
def analyze_country_differences(df, target):
    """
    分析不同国家组合的Deal multiple差异
    返回包含ANOVA表格的完整结果
    """
    # 数据准备
    temp_df = df[country_vars + [target]].dropna()
    temp_df['country_pair'] = temp_df['tar_country_code'] + '_' + temp_df['acq_country_code']
    
    # 过滤样本量不足的组合（至少5个样本）
    country_counts = temp_df['country_pair'].value_counts()
    valid_pairs = country_counts[country_counts >= 5].index  # 降低样本量阈值
    temp_df = temp_df[temp_df['country_pair'].isin(valid_pairs)]
    
    if len(temp_df) < 3 or len(valid_pairs) < 2:
        print(f"{target} 没有足够的数据进行国家间比较")
        return None
    
    # 计算描述性统计
    country_stats = temp_df.groupby('country_pair')[target].agg(['mean', 'count', 'std', 'median'])
    
    # 生成ANOVA表格
    model = ols(f'{target} ~ C(country_pair)', data=temp_df).fit()
    anova_table = sm.stats.anova_lm(model, typ=2)
    
    # 添加事后检验（Tukey HSD）
    from statsmodels.stats.multicomp import pairwise_tukeyhsd
    tukey = pairwise_tukeyhsd(temp_df[target], temp_df['country_pair'])
    
    return {
        'target': target,
        'country_stats': country_stats.sort_values('mean', ascending=False),
        'anova_table': anova_table,
        'tukey_results': tukey,
        'top_combination': country_stats['mean'].idxmax(),
        'bottom_combination': country_stats['mean'].idxmin(),
        'global_mean': temp_df[target].mean(),
        'sample_size': len(temp_df)
    }

# 4. 定义目标变量
target_multipliers = [
    'pre_rev_mul_ly', 'pre_ebitda_mul_ly', 'pre_ebit_mul_ly', 
    'pre_pbt_mul_ly', 'pre_pat_mul_ly'
]

# 5. 加载数据
df = pd.read_stata(r"C:/Users/FM/OneDrive/MA/acquisition/mergedtemp.dta")

# 6. 执行分析
country_results = {}
detailed_stats = pd.DataFrame()

for target in tqdm(target_multipliers, desc="国家比较分析"):
    result = analyze_country_differences(df, target)
    if result is not None:
        country_results[target] = result
        
        # 打印关键结果
        print(f"\n=== {target} ===")
        print(f"有效国家组合数: {len(result['country_stats'])}")
        print(f"总样本量: {result['sample_size']}")
        print("\nANOVA 结果:")
        print(result['anova_table'])
        
        # 保存详细统计
        stats_df = result['country_stats'].copy()
        stats_df['target'] = target
        detailed_stats = pd.concat([detailed_stats, stats_df])
        
        # 可视化
        plt.figure(figsize=(12,6))
        top_10 = result['country_stats'].head(10)
        sns.barplot(x=top_10.index, y='mean', data=top_10)
        plt.title(f"{target} 国家组合比较\nANOVA p={result['anova_table']['PR(>F)'][0]:.3f}")
        plt.xticks(rotation=45)
        plt.ylabel("平均倍数")
        save_plot(plt.gcf(), f"country_compare_{target}.png")
        plt.show()

# 7. 保存结果
output_path = r"C:\Users\FM\OneDrive\MA\acquisition\results"
os.makedirs(output_path, exist_ok=True)

if country_results:
    # 保存ANOVA结果
    anova_results = pd.concat({
        target: res['anova_table'] 
        for target, res in country_results.items()
    })
    anova_results.to_excel(os.path.join(output_path, "anova_results.xlsx"))
    
    # 保存Tukey检验结果
    with open(os.path.join(output_path, "tukey_test_results.txt"), "w") as f:
        for target, res in country_results.items():
            f.write(f"\n\n===== {target} =====\n")
            f.write(str(res['tukey_results']))
    
    # 保存国家统计
    detailed_stats.to_excel(os.path.join(output_path, "country_stats.xlsx"))
    
    print(f"\n分析完成！结果已保存至: {output_path}")