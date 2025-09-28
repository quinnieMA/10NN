# -*- coding: utf-8 -*-
"""
Created on Sat Aug 23 13:46:29 2025

@author: Lenovo
Core Analysis Focus
    Legal system impact on M&A valuation multiples
    Comparative analysis between country-level and legal system effects
    Legal System Classification

Four major legal families:
    Common law (US, UK, AU, etc.)
    Civil law (FR, IT, JP, CN, etc.)
    German law (DE, AT, CH)
    Scandinavian law (SE, DK, NO)

Maps country codes to legal systems
Dual Analysis Framework
Country-level analysis: Acquirer-target country pairs
Legal system analysis: Legal system combinations
Both use same statistical methodology

Statistical Methodology

    ANOVA for group differences
    Tukey HSD post-hoc tests
    Minimum 5 samples per group requirement
    Descriptive statistics (mean, median, std)

Key Target Variable
    Focuses on modelled_fee_income multiple
    (Other multiples commented out for focused analysis)

Output Features

    Separate result files for country and legal system analyses
    Visualizations: Bar charts for top combinations
    Statistical tables and test results exported
    Automated file organization

Research Application

    Examines institutional factors in M&A pricing
    Compares legal system effects vs. country-specific effects
    Provides cross-jurisdictional valuation insights
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

# 2. 法律体系分类映射
legal_system_mapping = {
    # Common Law Countries
    'US': 'common_law', 'GB': 'common_law', 'UK': 'common_law', 'AU': 'common_law',
    'CA': 'common_law', 'NZ': 'common_law', 'IE': 'common_law', 'IN': 'common_law',
    'SG': 'common_law', 'HK': 'common_law', 'MY': 'common_law', 'NG': 'common_law',
    'ZA': 'common_law', 'JM': 'common_law', 'BS': 'common_law', 'BB': 'common_law',
    
    # Civil Law Countries (excluding German and Scandinavian)
    'FR': 'civil_law', 'IT': 'civil_law', 'ES': 'civil_law', 'PT': 'civil_law',
    'NL': 'civil_law', 'BE': 'civil_law', 'LU': 'civil_law', 'BR': 'civil_law',
    'MX': 'civil_law', 'AR': 'civil_law', 'CL': 'civil_law', 'CO': 'civil_law',
    'PE': 'civil_law', 'VE': 'civil_law', 'PH': 'civil_law', 'ID': 'civil_law',
    'TH': 'civil_law', 'VN': 'civil_law', 'PL': 'civil_law', 'CZ': 'civil_law',
    'HU': 'civil_law', 'RO': 'civil_law', 'GR': 'civil_law', 'TR': 'civil_law',
    'JP': 'civil_law', 'KR': 'civil_law', 'CN': 'civil_law', 'TW': 'civil_law',
    
    # German Legal System Countries
    'DE': 'german_law', 'AT': 'german_law', 'CH': 'german_law', 'LI': 'german_law',
    
    # Scandinavian Legal System Countries
    'SE': 'scandinavian_law', 'DK': 'scandinavian_law', 'NO': 'scandinavian_law',
    'FI': 'scandinavian_law', 'IS': 'scandinavian_law'
}

def classify_legal_system(country_code):
    """根据国家代码分类法律体系"""
    if pd.isna(country_code):
        return 'unknown'
    return legal_system_mapping.get(str(country_code).upper(), 'other')

# 3. 定义保存图像函数
def save_plot(fig, filename):
    """保存图像到指定路径"""
    output_dir = r"D:\OneDrive\MA\acquisition\results"
    os.makedirs(output_dir, exist_ok=True)
    try:
        fig.savefig(os.path.join(output_dir, filename), 
                   dpi=300, bbox_inches='tight')
        plt.close()
    except Exception as e:
        print(f"保存 {filename} 时出错: {str(e)}")

# 4. 国家比较分析函数（保持不变）
def analyze_country_differences(df, target):
    """
    分析不同国家组合的Deal multiple差异
    返回包含ANOVA表格的完整结果
    """
    temp_df = df[country_vars + [target]].dropna()
    temp_df['country_pair'] = temp_df['tar_country_code'] + '_' + temp_df['acq_country_code']
    
    country_counts = temp_df['country_pair'].value_counts()
    valid_pairs = country_counts[country_counts >= 5].index
    temp_df = temp_df[temp_df['country_pair'].isin(valid_pairs)]
    
    if len(temp_df) < 3 or len(valid_pairs) < 2:
        print(f"{target} 没有足够的数据进行国家间比较")
        return None
    
    country_stats = temp_df.groupby('country_pair')[target].agg(['mean', 'count', 'std', 'median'])
    
    model = ols(f'{target} ~ C(country_pair)', data=temp_df).fit()
    anova_table = sm.stats.anova_lm(model, typ=2)
    
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

# 5. 法律体系比较分析函数（新增）
def analyze_legal_system_differences(df, target):
    """
    分析不同法律体系组合的Deal multiple差异
    """
    # 添加法律体系分类
    temp_df = df[country_vars + [target]].dropna()
    temp_df['tar_legal'] = temp_df['tar_country_code'].apply(classify_legal_system)
    temp_df['acq_legal'] = temp_df['acq_country_code'].apply(classify_legal_system)
    temp_df['legal_pair'] = temp_df['tar_legal'] + '_' + temp_df['acq_legal']
    
    # 过滤样本量不足的组合
    legal_counts = temp_df['legal_pair'].value_counts()
    valid_pairs = legal_counts[legal_counts >= 5].index
    temp_df = temp_df[temp_df['legal_pair'].isin(valid_pairs)]
    
    if len(temp_df) < 3 or len(valid_pairs) < 2:
        print(f"{target} 没有足够的数据进行法律体系间比较")
        return None
    
    # 计算描述性统计
    legal_stats = temp_df.groupby('legal_pair')[target].agg(['mean', 'count', 'std', 'median'])
    
    # ANOVA分析
    model = ols(f'{target} ~ C(legal_pair)', data=temp_df).fit()
    anova_table = sm.stats.anova_lm(model, typ=2)
    
    # Tukey HSD事后检验
    from statsmodels.stats.multicomp import pairwise_tukeyhsd
    tukey = pairwise_tukeyhsd(temp_df[target], temp_df['legal_pair'])
    
    return {
        'target': target,
        'legal_stats': legal_stats.sort_values('mean', ascending=False),
        'anova_table': anova_table,
        'tukey_results': tukey,
        'top_combination': legal_stats['mean'].idxmax(),
        'bottom_combination': legal_stats['mean'].idxmin(),
        'global_mean': temp_df[target].mean(),
        'sample_size': len(temp_df)
    }

# 6. 定义目标变量
target_multipliers = [
    #'pre_rev_mul_ly', 'pre_ebitda_mul_ly', 'pre_ebit_mul_ly', 'pre_pbt_mul_ly', 'pre_pat_mul_ly',
    'modelled_fee_income'
]

# 7. 加载数据
df = pd.read_stata(r"D:/OneDrive/MA/acquisition/mergedtemp.dta")

# 8. 执行国家比较分析（保持不变）
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

# 9. 执行法律体系比较分析（新增）
legal_results = {}
legal_detailed_stats = pd.DataFrame()

for target in tqdm(target_multipliers, desc="法律体系比较分析"):
    result = analyze_legal_system_differences(df, target)
    if result is not None:
        legal_results[target] = result
        
        # 打印关键结果
        print(f"\n=== {target} (法律体系) ===")
        print(f"有效法律体系组合数: {len(result['legal_stats'])}")
        print(f"总样本量: {result['sample_size']}")
        print("\nANOVA 结果:")
        print(result['anova_table'])
        
        # 保存详细统计
        stats_df = result['legal_stats'].copy()
        stats_df['target'] = target
        legal_detailed_stats = pd.concat([legal_detailed_stats, stats_df])
        
        # 可视化
        plt.figure(figsize=(10,6))
        sns.barplot(x=result['legal_stats'].index, y='mean', data=result['legal_stats'])
        plt.title(f"{target} 法律体系组合比较\nANOVA p={result['anova_table']['PR(>F)'][0]:.3f}")
        plt.xticks(rotation=45)
        plt.ylabel("平均倍数")
        save_plot(plt.gcf(), f"legal_compare_{target}.png")
        plt.show()

# 10. 保存结果
output_path = r"D:\OneDrive\MA\acquisition\results1"
os.makedirs(output_path, exist_ok=True)

# 保存国家比较结果
if country_results:
    anova_results = pd.concat({
        target: res['anova_table'] 
        for target, res in country_results.items()
    })
    anova_results.to_excel(os.path.join(output_path, "country_anova_results.xlsx"))
    
    with open(os.path.join(output_path, "country_tukey_test_results.txt"), "w") as f:
        for target, res in country_results.items():
            f.write(f"\n\n===== {target} =====\n")
            f.write(str(res['tukey_results']))
    
    detailed_stats.to_excel(os.path.join(output_path, "country_detailed_stats.xlsx"))

# 保存法律体系比较结果
if legal_results:
    legal_anova_results = pd.concat({
        target: res['anova_table'] 
        for target, res in legal_results.items()
    })
    legal_anova_results.to_excel(os.path.join(output_path, "legal_anova_results.xlsx"))
    
    with open(os.path.join(output_path, "legal_tukey_test_results.txt"), "w") as f:
        for target, res in legal_results.items():
            f.write(f"\n\n===== {target} =====\n")
            f.write(str(res['tukey_results']))
    
    legal_detailed_stats.to_excel(os.path.join(output_path, "legal_detailed_stats.xlsx"))

print(f"\n分析完成！结果已保存至: {output_path}")