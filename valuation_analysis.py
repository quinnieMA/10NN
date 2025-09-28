''' Core Analysis Modules

Country Difference Analysis

    Compares valuation multiples across country combinations (acquirer + target)   
    Uses ANOVA and Tukey post-hoc tests   
    Only analyzes country pairs with ≥5 samples   
    Generates charts for top 10 country combinations    
    Vertical Relationship Analysis

Classifies M&A types using SIC codes:

    Horizontal (same industry)   
    Upstream (to supply chain upstream)    
    Downstream (to supply chain downstream)    
    Conglomerate (unrelated industries)

Statistical tests:

    ANOVA    
    Kruskal-Wallis non-parametric test    
    Mann-Whitney U test (vertical vs horizontal focus)

Visualization Outputs

    Box plots for multiple distributions   
    Bar charts for mean comparisons   
    Financial feature comparisons (revenue, EBITDA, deal value)

Application Scenarios

    Empirical finance research testing:
    Cross-border M&A valuation differences   
    Value creation in vertical integration   
    Supports decision-making for investment banks and PE firms

Technical Features
    
    Automated statistical analysis pipeline    
    Multiple hypothesis testing correction    
    Outlier handling (showfliers=False)    
    Automated result saving and visualization   
    Provides empirical evidence for M&A pricing and strategy selection'''

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import f_oneway, kruskal, mannwhitneyu
import statsmodels.api as sm
from statsmodels.formula.api import ols
from statsmodels.stats.multicomp import pairwise_tukeyhsd
import os
from tqdm import tqdm

# 设置中文字体和样式
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
sns.set_style('whitegrid')

# 定义全局变量
country_vars = ['tar_country_code', 'acq_country_code']
target_multipliers = [
    'pre_rev_mul_ly', 'pre_ebitda_mul_ly', 'pre_ebit_mul_ly', 
    'pre_pbt_mul_ly', 'pre_pat_mul_ly'
]
output_path = r"D:\OneDrive\MA\acquisition\results"
os.makedirs(output_path, exist_ok=True)

# 工具函数
def save_plot(fig, filename):
    """保存图像到指定路径"""
    try:
        fig.savefig(os.path.join(output_path, filename), 
                   dpi=300, bbox_inches='tight')
        plt.close()
    except Exception as e:
        print(f"保存 {filename} 时出错: {str(e)}")

def get_vertical_relationship(row):
    """确定上下游关系"""
    tar_sic = str(row['tar_primary_sic_code']).strip()
    acq_sic = str(row['acq_primary_sic_code']).strip()
    
    if tar_sic == acq_sic or tar_sic == 'nan' or acq_sic == 'nan':
        return 'Horizontal'
    
    def get_sector(sic):
        try:
            num = int(sic.ljust(4,'0')[:4])  # 确保4位代码
            if 100 <= num < 2000: return '1000-1999'
            elif 2000 <= num < 3000: return '2000-2999'
            elif 3000 <= num < 4000: return '3000-3999'
            elif 4000 <= num < 5000: return '4000-4999'
            elif 5000 <= num < 6000: return '5000-5999'
            elif 6000 <= num < 7000: return '6000-6999'
            elif 7000 <= num < 8000: return '7000-7999'
            else: return 'Other'
        except:
            return 'Unknown'
    
    tar_sec, acq_sec = get_sector(tar_sic), get_sector(acq_sic)
    
    # SIC行业层级关系定义
    sic_hierarchy = {
        '1000-1999': ['2000-2999', '3000-3999'],  # 农业 -> 加工/制造
        '2000-2999': ['3000-3999', '5000-5999'],  # 加工 -> 制造/批发
        '3000-3999': ['4000-4999', '5000-5999'],  # 制造 -> 运输/批发
        '5000-5999': ['7000-7999'],               # 批发 -> 服务
        '6000-6999': ['6000-6999']                # 金融同业
    }
    
    if tar_sec == acq_sec:
        return 'Horizontal'
    elif acq_sec in sic_hierarchy.get(tar_sec, []):
        return 'Downstream'
    elif tar_sec in sic_hierarchy.get(acq_sec, []):
        return 'Upstream'
    else:
        return 'Conglomerate'

def analyze_country_differences(df, target):
    """分析国家间并购倍数差异"""
    temp_df = df[country_vars + [target]].dropna()
    temp_df['country_pair'] = temp_df['tar_country_code'] + '_' + temp_df['acq_country_code']
    
    # 样本过滤
    country_counts = temp_df['country_pair'].value_counts()
    valid_pairs = country_counts[country_counts >= 5].index
    temp_df = temp_df[temp_df['country_pair'].isin(valid_pairs)]
    
    if len(temp_df) < 3 or len(valid_pairs) < 2:
        print(f"{target} 没有足够的数据进行国家间比较")
        return None
    
    # 统计分析
    country_stats = temp_df.groupby('country_pair')[target].agg(['mean', 'count', 'std', 'median'])
    model = ols(f'{target} ~ C(country_pair)', data=temp_df).fit()
    
    return {
        'target': target,
        'country_stats': country_stats.sort_values('mean', ascending=False),
        'anova_table': sm.stats.anova_lm(model, typ=2),
        'tukey_results': pairwise_tukeyhsd(temp_df[target], temp_df['country_pair']),
        'sample_size': len(temp_df)
    }

def perform_vertical_analysis(df):
    """执行上下游并购分析"""
    # 添加vertical_relation列
    df['vertical_relation'] = df.apply(get_vertical_relationship, axis=1)
    
    # 1. 描述性统计
    desc_stats = df.groupby('vertical_relation')[target_multipliers].agg(['mean', 'median', 'std', 'count'])
    print("\n各类型并购的倍数描述统计:")
    print(desc_stats)
    
    # 2. 方差分析（ANOVA）
    print("\nANOVA检验结果:")
    anova_results = {}
    for target in target_multipliers:
        groups = [group[target].values for name, group in df.groupby('vertical_relation')]
        f_val, p_val = f_oneway(*groups)
        anova_results[target] = {'F': f_val, 'p': p_val}
        print(f"{target}: F={f_val:.2f}, p={p_val:.4f}")
    
    # 3. 非参数检验（Kruskal-Wallis）
    print("\nKruskal-Wallis检验结果:")
    kruskal_results = {}
    for target in target_multipliers:
        groups = [group[target].values for name, group in df.groupby('vertical_relation')]
        h_val, p_val = kruskal(*groups)
        kruskal_results[target] = {'H': h_val, 'p': p_val}
        print(f"{target}: H={h_val:.2f}, p={p_val:.4f}")
    
    # 4. 事后检验（仅当整体显著时）
    print("\n事后检验（Tukey HSD）:")
    for target in target_multipliers:
        if anova_results[target]['p'] < 0.05:
            print(f"\n{target} 的事后检验:")
            tukey = pairwise_tukeyhsd(df[target], df['vertical_relation'])
            print(tukey)
    
    # 5. 重点比较：上下游 vs 横向
    print("\n上下游 vs 横向并购比较（Mann-Whitney U检验）:")
    for target in target_multipliers:
        vertical = df[df['vertical_relation'].isin(['Upstream', 'Downstream'])][target]
        horizontal = df[df['vertical_relation'] == 'Horizontal'][target]
        u_val, p_val = mannwhitneyu(vertical, horizontal)
        print(f"{target}: U={u_val:.0f}, p={p_val:.4f}")
    
    # 6. 可视化
    plot_vertical_comparison(df)

def plot_vertical_comparison(df):
    """绘制上下游比较可视化"""
    # 倍数分布箱线图
    for target in target_multipliers:
        plt.figure(figsize=(12, 6))
        sns.boxplot(data=df, x='vertical_relation', y=target,
                   order=['Horizontal', 'Upstream', 'Downstream', 'Conglomerate'],
                   showfliers=False)
        plt.title(f"{target} 在不同并购类型中的分布")
        save_plot(plt.gcf(), f"vertical_{target}_boxplot.png")
    
    # 均值比较条形图
    mean_df = df.groupby('vertical_relation')[target_multipliers].mean().reset_index()
    mean_df = pd.melt(mean_df, id_vars='vertical_relation', 
                     value_vars=target_multipliers,
                     var_name='multiple', value_name='mean_value')
    
    plt.figure(figsize=(14, 8))
    sns.barplot(data=mean_df, x='multiple', y='mean_value', hue='vertical_relation',
               hue_order=['Horizontal', 'Upstream', 'Downstream', 'Conglomerate'])
    plt.title("各类型并购的倍数均值比较")
    plt.ylabel("均值")
    plt.xticks(rotation=45)
    save_plot(plt.gcf(), "vertical_multiple_comparison.png")
    
    # 财务特征比较
    financial_vars = [
        'pre_deal_tar_rev_rev_last_avail_', 'pre_deal_tar_ebitda_last_avail_y',
        'deal_value'
    ]
    plt.figure(figsize=(15, 10))
    for i, var in enumerate(financial_vars, 1):
        plt.subplot(2, 2, i)
        sns.boxplot(data=df, x='vertical_relation', y=var,
                   order=['Horizontal', 'Upstream', 'Downstream', 'Conglomerate'],
                   showfliers=False)
        plt.title(f"{var} 分布")
    save_plot(plt.gcf(), "vertical_financial_comparison.png")

# 主分析流程
def main_analysis():
    # 加载数据
    try:
        df = pd.read_stata(r"D:/OneDrive/MA/acquisition/mergedtemp.dta")
    except Exception as e:
        print(f"数据加载失败: {str(e)}")
        return
    
    # 国家比较分析
    country_results = {}
    detailed_stats = pd.DataFrame()
    
    for target in tqdm(target_multipliers, desc="国家比较分析"):
        result = analyze_country_differences(df, target)
        if result:
            country_results[target] = result
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
            plt.close()
    
    # 上下游分析
    perform_vertical_analysis(df)
    
    # 保存结果
    try:
        if country_results:
            pd.concat({k: v['anova_table'] for k,v in country_results.items()}).to_excel(
                os.path.join(output_path, "anova_results_ind.xlsx"))
            detailed_stats.to_excel(os.path.join(output_path, "country_stats.xlsx"))
        
        df.to_excel(os.path.join(output_path, "merged_analysis_results.xlsx"), index=False)
        print(f"\n所有分析结果已保存至: {output_path}")
    except Exception as e:
        print(f"结果保存失败: {str(e)}")

if __name__ == '__main__':
    main_analysis()