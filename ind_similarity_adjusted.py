# -*- coding: utf-8 -*-
"""
Created on Fri Aug 29 09:24:12 2025

@author: Lenovo
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
    
    # Output
    OUTPUT_FOLDER = "D:/OneDrive/MA/acquisition/similarity_results"
    TARGET_SIMILARITY = os.path.join(OUTPUT_FOLDER, "target_similarity_scores.csv")
    TARGET_TOP10_NEIGHBORS = os.path.join(OUTPUT_FOLDER, "target_top10_neighbors.csv")
    TARGET_SIMILARITY_MATRIX = os.path.join(OUTPUT_FOLDER, "target_similarity_matrix.npy")

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
# Similarity Calculation (Target-Target only)
# ========================

def calculate_target_similarity(target_texts):
    """
    Calculate cosine similarity between target documents only
    Returns similarity matrix and target IDs
    """
    # Get IDs and texts in order
    target_ids = list(target_texts.keys())
    documents = [target_texts[target_id] for target_id in target_ids]
    
    print(f"Total target documents for TF-IDF: {len(documents)}")
    
    # Create TF-IDF vectorizer with optimized parameters
    vectorizer = TfidfVectorizer(
        max_features=3000,  # Reduced features to save memory
        min_df=3,           # Ignore very rare terms
        max_df=0.7,         # Ignore very common terms
        ngram_range=(1, 2)  # Use unigrams and bigrams
    )
    
    # Fit and transform
    tfidf_matrix = vectorizer.fit_transform(documents)
    
    # Calculate cosine similarity
    print("Calculating cosine similarity matrix...")
    similarity_matrix = cosine_similarity(tfidf_matrix)
    
    return similarity_matrix, target_ids, vectorizer.get_feature_names_out()

def find_top_k_target_neighbors(similarity_matrix, target_ids, k=10):
    """
    Find top K most similar target documents for each target
    Returns only the top 10 neighbors to reduce file size
    """
    neighbors_data = []
    
    for i, target_id in enumerate(target_ids):
        # Get similarity scores for this target
        similarities = similarity_matrix[i]
        
        # Exclude self-similarity
        similarities[i] = -1
        
        # Get indices of top K neighbors
        top_k_indices = np.argsort(similarities)[-k:][::-1]
        
        # Store results only for top neighbors
        for rank, idx in enumerate(top_k_indices, 1):
            neighbors_data.append({
                'source_target': target_id.replace('target_', ''),
                'neighbor_target': target_ids[idx].replace('target_', ''),
                'similarity_score': similarities[idx],
                'rank': rank
            })
    
    return pd.DataFrame(neighbors_data)

def save_similarity_summary(similarity_matrix, target_ids, output_path):
    """
    Save summary statistics instead of full matrix to avoid large files
    """
    summary_data = []
    
    for i, target_id in enumerate(target_ids):
        # Get similarity scores for this target (excluding self)
        similarities = similarity_matrix[i]
        similarities[i] = -1  # Exclude self
        
        # Calculate summary statistics
        summary_data.append({
            'target_id': target_id.replace('target_', ''),
            'mean_similarity': np.mean(similarities[similarities >= 0]),
            'max_similarity': np.max(similarities),
            'min_similarity': np.min(similarities[similarities >= 0]),
            'std_similarity': np.std(similarities[similarities >= 0]),
            'num_similar_targets': len(similarities[similarities >= 0])
        })
    
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(output_path, index=False)
    return summary_df

# ========================
# Main Processing Pipeline (Target-Target only)
# ========================

def target_similarity_analysis_pipeline():
    """Main pipeline for target-target similarity analysis"""
    print("Starting target-target similarity analysis pipeline...")
    
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
        
        # 3. Calculate similarity matrix (target-target only)
        print("Calculating target-target cosine similarities...")
        similarity_matrix, all_target_ids, feature_names = calculate_target_similarity(target_texts)
        print(f"Similarity matrix shape: {similarity_matrix.shape}")
        print(f"Vocabulary size: {len(feature_names)}")
        
        # 4. Find top 10 neighbors for each target
        print("Finding top 10 target neighbors...")
        neighbors_df = find_top_k_target_neighbors(similarity_matrix, all_target_ids, k=10)
        neighbors_df.to_csv(DataPaths.TARGET_TOP10_NEIGHBORS, index=False)
        print(f"Top neighbors saved to {DataPaths.TARGET_TOP10_NEIGHBORS}")
        print(f"Top neighbors data shape: {neighbors_df.shape}")
        
        # 5. Save similarity summary (instead of full matrix)
        print("Saving similarity summary...")
        summary_df = save_similarity_summary(similarity_matrix, all_target_ids, DataPaths.TARGET_SIMILARITY)
        print(f"Similarity summary saved to {DataPaths.TARGET_SIMILARITY}")
        
        # 6. Save the similarity matrix in binary format (optional, for future use)
        np.save(DataPaths.TARGET_SIMILARITY_MATRIX, similarity_matrix)
        print(f"Similarity matrix saved to {DataPaths.TARGET_SIMILARITY_MATRIX}.npy")
        
        # 7. Summary statistics
        print("\n=== Summary Statistics ===")
        print(f"Mean similarity across all pairs: {np.mean(similarity_matrix):.4f}")
        print(f"Max similarity: {np.max(similarity_matrix):.4f}")
        print(f"Min similarity: {np.min(similarity_matrix):.4f}")
        
        # Show top 10 most similar target pairs
        top_pairs = neighbors_df.nlargest(10, 'similarity_score')
        print("\nTop 10 most similar target pairs:")
        for _, row in top_pairs.iterrows():
            print(f"Target {row['source_target']} - Neighbor {row['neighbor_target']}: {row['similarity_score']:.4f}")
            
        return summary_df, neighbors_df, similarity_matrix
        
    except Exception as e:
        print(f"Error in similarity analysis pipeline: {e}")
        import traceback
        traceback.print_exc()
        raise

# ========================
# Additional Analysis Functions
# ========================

def analyze_similarity_distribution(summary_df):
    """Analyze the distribution of similarity scores"""
    if len(summary_df) > 0:
        print("\n=== Similarity Distribution ===")
        print(summary_df['mean_similarity'].describe())
        
        # Plot histogram
        import matplotlib.pyplot as plt
        plt.figure(figsize=(10, 6))
        plt.hist(summary_df['mean_similarity'], bins=50, alpha=0.7)
        plt.title('Distribution of Mean Cosine Similarity Scores')
        plt.xlabel('Mean Cosine Similarity')
        plt.ylabel('Frequency')
        plt.grid(True, alpha=0.3)
        plt.show()

# ========================
# Main Execution
# ========================

if __name__ == "__main__":
    # Run the target-target similarity pipeline
    summary_results, neighbor_results, similarity_matrix = target_similarity_analysis_pipeline()
    
    # Additional analysis
    if len(summary_results) > 0:
        analyze_similarity_distribution(summary_results)
    
    print("\nTarget-target similarity analysis completed!")