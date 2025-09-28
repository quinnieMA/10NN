# -*- coding: utf-8 -*-
"""
并购分析综合工具 - 国家比较 & 上下游分析
Created on Sat Jul 26 20:17:23 2025
@author: FM
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import f_oneway
import statsmodels.api as sm
from statsmodels.formula.api import ols
from statsmodels.stats.multicomp import pairwise_tukeyhsd
import os
from tqdm import tqdm

# 设置中文字体和样式
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
sns.set_style('whitegrid')  # 使用seaborn的样式设置替代plt.style.use

# 1. 定义全局变量
country_vars = ['tar_country_code', 'acq_country_code']
target_multipliers = [
    'pre_rev_mul_ly', 'pre_ebitda_mul_ly', 'pre_ebit_mul_ly', 
    'pre_pbt_mul_ly', 'pre_pat_mul_ly'
]
output_path = r"D:\OneDrive\MA\acquisition\results"
os.makedirs(output_path, exist_ok=True)

# 2. 工具函数
def save_plot(fig, filename):
    """保存图像到指定路径"""
    try:
        fig.savefig(os.path.join(output_path, filename), 
                   dpi=300, bbox_inches='tight')
        plt.close()
    except Exception as e:
        print(f"保存 {filename} 时出错: {str(e)}")

# 3. 国家比较分析
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

# 4. 上下游并购分析
def analyze_vertical_mergers(df):
    """分析上下游并购关系"""
    # SIC行业层级关系定义 (示例)
    sic_hierarchy = {
        '1000-1999': ['2000-2999', '3000-3999'],  # 农业 -> 加工/制造
        '2000-2999': ['3000-3999', '5000-5999'],  # 加工 -> 制造/批发
        '3000-3999': ['4000-4999', '5000-5999'],  # 制造 -> 运输/批发
        '5000-5999': ['7000-7999'],               # 批发 -> 服务
        '6000-6999': ['6000-6999']                # 金融同业
    }

    def get_vertical_relationship(row):
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
        
        if tar_sec == acq_sec:
            return 'Horizontal'
        elif acq_sec in sic_hierarchy.get(tar_sec, []):
            return 'Downstream'
        elif tar_sec in sic_hierarchy.get(acq_sec, []):
            return 'Upstream'
        else:
            return 'Conglomerate'
    
    df['vertical_relation'] = df.apply(get_vertical_relationship, axis=1)
    return df['vertical_relation'].value_counts(normalize=True) * 100

# 5. 主分析流程
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
    try:
        vertical_result = analyze_vertical_mergers(df)
        print("\n上下游并购分布:")
        print(vertical_result)
        
        plt.figure(figsize=(10,6))
        sns.countplot(data=df, y='vertical_relation', 
                     order=['Horizontal', 'Upstream', 'Downstream', 'Conglomerate'],
                     palette='viridis')
        plt.title("并购交易垂直方向分布")
        save_plot(plt.gcf(), "vertical_merger_dist.png")
        plt.close()
    except Exception as e:
        print(f"上下游分析出错: {str(e)}")
    
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
    