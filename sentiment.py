# -*- coding: utf-8 -*-
"""
Created on Wed Jul 23 16:54:43 2025

@author: FM
"""

import os
import pandas as pd
import cntext as ct

# 设置路径
FOLDER = "C:/Users/FM/OneDrive/MA/acquisition/NLP_acq"
trigram_folder = os.path.join(FOLDER, 'processed/trigram')
id_file = os.path.join(FOLDER, 'processed/document_ids.txt')
output_folder = os.path.join(FOLDER, 'sentiment_results')

# 创建输出文件夹
os.makedirs(output_folder, exist_ok=True)


# 定义要排除的中文相关词典
CHINESE_DICTS = [
    'ChineseEmoBank.pkl',
    'ChineseFinancialFormalUnformalSentiment.pkl',
    'Chinese_Digitalization.pkl',
    'Chinese_FLS.pkl',
    'Chinese_Loughran_McDonald_Financial_Sentiment.pkl',
    'DUTIR.pkl',
    'HOWNET.pkl'
]

# 获取所有非中文词典
all_dicts = [
    'ADV_CONJ.pkl',
    'AFINN.pkl',
    'ANEW.pkl',
    'Concreteness.pkl',
    'geninqposneg.pkl',
    'HuLiu.pkl',
    'Loughran_McDonald_Financial_Sentiment.pkl',
    'LSD2015.pkl',
    'NRC.pkl',
    'sentiws.pkl',
    'STOPWORDS.pkl'
]

# 读取文档ID
with open(id_file, 'r', encoding='utf-8') as f:
    ids = [line.strip('\n') for line in f.readlines()]

# 获取trigram文件列表
file_list = sorted([f for f in os.listdir(trigram_folder) if f.endswith('.txt')])
if len(ids) != len(file_list):
    print(f"警告: ID数量({len(ids)})与文件数量({len(file_list)})不匹配")

# 处理每个词典
for dict_name in all_dicts:
    print(f"\n正在处理词典: {dict_name}")
    
    try:
        # 加载词典
        dict_data = ct.load_pkl_dict(dict_name)
        dict_key = list(dict_data.keys())[0]  # 获取词典主键
        dictionary = dict_data[dict_key]
        
        # 初始化结果存储
        results = []
        
        # 处理每个文件
        for filename, doc_id in zip(file_list, ids):
            filepath = os.path.join(trigram_folder, filename)
            
            # 读取文件内容
            with open(filepath, 'r', encoding='utf-8') as f:
                text = f.read()
            
            # 进行情感分析
            sentiment_result = ct.sentiment(
                text=text,
                diction=dictionary,
                lang='english'  # 大多数词典是英文的
            )
            
            # 创建记录
            record = {'document_id': doc_id}
            
            # 动态添加所有结果指标
            for key, value in sentiment_result.items():
                if '_num' in key or 'word_num' in key or 'sentence_num' in key:
                    # 简化键名，去掉'_num'后缀
                    clean_key = key.replace('_num', '')
                    record[clean_key] = value
            
            results.append(record)
        
        # 创建DataFrame
        df = pd.DataFrame(results)
        df.set_index('document_id', inplace=True)
        
        # 保存结果
        output_file = os.path.join(output_folder, f"{dict_name.replace('.pkl', '')}_results.csv")
        df.to_csv(output_file)
        print(f"已保存: {output_file}")
        print(f"结果形状: {df.shape}")
        
    except Exception as e:
        print(f"处理词典 {dict_name} 时出错: {str(e)}")
        continue

print("\n所有词典处理完成！")