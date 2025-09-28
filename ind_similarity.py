# -*- coding: utf-8 -*-
"""
Created on Wed Aug 27 2025

@author: Lenovo
Description: Calculate cosine similarity between target and acquirer overviews using 10NN method
"""

import os
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer



from sklearn.metrics.pairwise import cosine_similarity
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# ========================
# Path Configuration
# ========================
class DataPaths:
    # Target overviews
    TARGET_FOLDER = "D:/OneDrive/MA/acquisition/NLP_tar_overview/processed/trigram"
    TARGET_IDS = "D:/OneDrive/MA/acquisition/NLP_tar_overview/processed/document_ids.txt"
    
    # Acquirer overviews  
    ACQUIRER_FOLDER = "D:/OneDrive/MA/acquisition/NLP_acq_overview/processed/trigram"
    ACQUIRER_CORPUS = "D:/OneDrive/MA/acquisition/NLP_acq_overview/processed/document_trigram.txt"
    
    # Output
    OUTPUT_FOLDER = "D:/OneDrive/MA/acquisition/similarity_results"
    SIMILARITY_SCORES = os.path.join(OUTPUT_FOLDER, "cosine_similarity_scores.csv")
    TOP10_NEIGHBORS = os.path.join(OUTPUT_FOLDER, "top10_neighbors.csv")

# ========================
# Text Processing Utilities
# ========================

def load_document_ids(file_path):
    """Load document IDs from file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f.readlines() if line.strip()]

def read_text_files(folder_path, file_prefix=''):
    """Read all text files from folder and return as dictionary {filename: text}"""
    texts = {}
    for filename in os.listdir(folder_path):
        if filename.endswith('.txt'):
            filepath = os.path.join(folder_path, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if content:
                        # Remove file extension for ID
                        doc_id = filename.replace('.txt', '')
                        if file_prefix:
                            doc_id = f"{file_prefix}_{doc_id}"
                        texts[doc_id] = content
            except Exception as e:
                print(f"Error reading {filename}: {e}")
    return texts

# ========================
# Similarity Calculation
# ========================

def calculate_cosine_similarity(target_texts, acquirer_texts):
    """
    Calculate cosine similarity between all target-acquirer pairs
    Returns similarity matrix and feature names
    """
    # Combine all texts
    all_texts = {}
    all_texts.update(target_texts)
    all_texts.update(acquirer_texts)
    
    # Get IDs and texts in order
    doc_ids = list(all_texts.keys())
    documents = [all_texts[doc_id] for doc_id in doc_ids]
    
    print(f"Total documents for TF-IDF: {len(documents)}")
    
    # Create TF-IDF vectorizer
    vectorizer = TfidfVectorizer(
        max_features=5000,  # Limit features to avoid memory issues
        min_df=2,           # Ignore terms that appear in less than 2 documents
        max_df=0.8          # Ignore terms that appear in more than 80% of documents
    )
    
    # Fit and transform
    tfidf_matrix = vectorizer.fit_transform(documents)
    
    # Calculate cosine similarity
    similarity_matrix = cosine_similarity(tfidf_matrix)
    
    return similarity_matrix, doc_ids, vectorizer.get_feature_names_out()

def find_top_k_neighbors(similarity_matrix, doc_ids, k=10):
    """
    Find top K most similar documents for each document
    """
    neighbors_data = []
    
    for i, doc_id in enumerate(doc_ids):
        # Get similarity scores for this document
        similarities = similarity_matrix[i]
        
        # Exclude self-similarity
        similarities[i] = -1
        
        # Get indices of top K neighbors
        top_k_indices = np.argsort(similarities)[-k:][::-1]
        
        # Store results
        for rank, idx in enumerate(top_k_indices, 1):
            neighbors_data.append({
                'source_doc': doc_id,
                'target_doc': doc_ids[idx],
                'similarity_score': similarities[idx],
                'rank': rank
            })
    
    return pd.DataFrame(neighbors_data)

def calculate_target_acquirer_similarity(similarity_matrix, target_ids, acquirer_ids, doc_ids):
    """
    Calculate similarity scores between target and acquirer documents
    """
    # Create mapping from doc_id to index
    doc_to_index = {doc_id: idx for idx, doc_id in enumerate(doc_ids)}
    
    similarity_scores = []
    
    # Debug: print some info
    print(f"Total doc_ids: {len(doc_ids)}")
    print(f"Target IDs to match: {len(target_ids)}")
    print(f"Acquirer IDs to match: {len(acquirer_ids)}")
    
    # Sample some IDs to see the format
    print("Sample target IDs:", target_ids[:5] if target_ids else "None")
    print("Sample acquirer IDs:", acquirer_ids[:5] if acquirer_ids else "None")
    print("Sample doc_ids:", doc_ids[:5] if doc_ids else "None")
    
    target_count = 0
    acquirer_count = 0
    
    for target_id in target_ids:
        # Create the formatted target ID as it appears in doc_ids
        formatted_target_id = f"target_{target_id}"
        if formatted_target_id in doc_to_index:
            target_count += 1
            target_idx = doc_to_index[formatted_target_id]
            
            for acquirer_id in acquirer_ids:
                # Create the formatted acquirer ID as it appears in doc_ids
                formatted_acquirer_id = f"acq_{acquirer_id}"
                if formatted_acquirer_id in doc_to_index:
                    acquirer_count += 1
                    acquirer_idx = doc_to_index[formatted_acquirer_id]
                    score = similarity_matrix[target_idx][acquirer_idx]
                    
                    similarity_scores.append({
                        'target_id': target_id,
                        'acquirer_id': acquirer_id,
                        'cosine_similarity': score
                    })
    
    print(f"Found {target_count} target matches and {acquirer_count} acquirer matches")
    print(f"Total similarity pairs: {len(similarity_scores)}")
    
    return pd.DataFrame(similarity_scores)

# ========================
# Main Processing Pipeline
# ========================

def similarity_analysis_pipeline():
    """Main pipeline for similarity analysis"""
    print("Starting similarity analysis pipeline...")
    
    # Create output directory
    os.makedirs(DataPaths.OUTPUT_FOLDER, exist_ok=True)
    
    try:
        # 1. Load document IDs
        print("Loading document IDs...")
        target_ids = load_document_ids(DataPaths.TARGET_IDS)
        print(f"Loaded {len(target_ids)} target IDs")
        
        # 2. Load target texts
        print("Loading target overviews...")
        target_texts = read_text_files(DataPaths.TARGET_FOLDER, 'target')
        print(f"Loaded {len(target_texts)} target documents")
        
        # 3. Load acquirer texts
        print("Loading acquirer overviews...")
        acquirer_texts = read_text_files(DataPaths.ACQUIRER_FOLDER, 'acq')
        print(f"Loaded {len(acquirer_texts)} acquirer documents")
        
        # 4. Calculate similarity matrix
        print("Calculating cosine similarities...")
        similarity_matrix, all_doc_ids, feature_names = calculate_cosine_similarity(
            target_texts, acquirer_texts
        )
        print(f"Similarity matrix shape: {similarity_matrix.shape}")
        print(f"Vocabulary size: {len(feature_names)}")
        
        # 5. Find top 10 neighbors for each document
        print("Finding top 10 neighbors...")
        neighbors_df = find_top_k_neighbors(similarity_matrix, all_doc_ids, k=10)
        neighbors_df.to_csv(DataPaths.TOP10_NEIGHBORS, index=False)
        print(f"Top neighbors saved to {DataPaths.TOP10_NEIGHBORS}")
        
        # 6. Calculate target-acquirer specific similarities
        print("Calculating target-acquirer similarities...")
        # Extract pure target and acquirer IDs (without prefixes)
        pure_target_ids = [tid.replace('target_', '') for tid in target_texts.keys()]
        pure_acquirer_ids = [aid.replace('acq_', '') for aid in acquirer_texts.keys()]
        
        similarity_scores = calculate_target_acquirer_similarity(
            similarity_matrix, pure_target_ids, pure_acquirer_ids, all_doc_ids
        )
        
        # 7. Save results
        similarity_scores.to_csv(DataPaths.SIMILARITY_SCORES, index=False)
        print(f"Similarity scores saved to {DataPaths.SIMILARITY_SCORES}")
        print(f"Total target-acquirer pairs: {len(similarity_scores)}")
        
        # 8. Summary statistics (only if we have results)
        if len(similarity_scores) > 0:
            print("\n=== Summary Statistics ===")
            print(f"Mean similarity: {similarity_scores['cosine_similarity'].mean():.4f}")
            print(f"Max similarity: {similarity_scores['cosine_similarity'].max():.4f}")
            print(f"Min similarity: {similarity_scores['cosine_similarity'].min():.4f}")
            
            # Show top 10 most similar pairs
            top_pairs = similarity_scores.nlargest(10, 'cosine_similarity')
            print("\nTop 10 most similar pairs:")
            for _, row in top_pairs.iterrows():
                print(f"Target {row['target_id']} - Acquirer {row['acquirer_id']}: {row['cosine_similarity']:.4f}")
        else:
            print("No target-acquirer pairs found. Check ID matching.")
            
        return similarity_scores, neighbors_df
        
    except Exception as e:
        print(f"Error in similarity analysis pipeline: {e}")
        import traceback
        traceback.print_exc()
        raise

# ========================
# Additional Analysis Functions
# ========================

def analyze_similarity_distribution(similarity_scores):
    """Analyze the distribution of similarity scores"""
    if len(similarity_scores) > 0:
        print("\n=== Similarity Distribution ===")
        print(similarity_scores['cosine_similarity'].describe())
        
        # Plot histogram
        import matplotlib.pyplot as plt
        plt.figure(figsize=(10, 6))
        plt.hist(similarity_scores['cosine_similarity'], bins=50, alpha=0.7)
        plt.title('Distribution of Cosine Similarity Scores')
        plt.xlabel('Cosine Similarity')
        plt.ylabel('Frequency')
        plt.grid(True, alpha=0.3)
        plt.show()

# ========================
# Main Execution
# ========================

if __name__ == "__main__":
    # Run the complete pipeline
    similarity_results, neighbor_results = similarity_analysis_pipeline()
    
    # Additional analysis
    if len(similarity_results) > 0:
        analyze_similarity_distribution(similarity_results)
    
    print("\nPipeline completed!")