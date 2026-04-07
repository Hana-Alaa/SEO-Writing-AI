import logging
import numpy as np
import torch
from typing import List, Union
from sentence_transformers import SentenceTransformer, util

logger = logging.getLogger(__name__)

class SemanticService:
    def __init__(self, model_name: str = "paraphrase-multilingual-MiniLM-L12-v2"):
        """
        Initializes the Semantic Intelligence Layer using Sentence-Transformers.
        Supports 50+ languages including Arabic.
        """
        try:
            logger.info(f"Loading Semantic Model: {model_name}...")
            # Use CPU by default to avoid CUDA dependency issues in some environments
            device = "cuda" if torch.cuda.is_available() else "cpu"
            self.model = SentenceTransformer(model_name, device=device)
            logger.info(f"Model loaded successfully on {device}.")
        except Exception as e:
            logger.error(f"Failed to load semantic model: {e}")
            self.model = None

    def calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculates cosine similarity between two texts."""
        if not self.model or not text1 or not text2:
            return 0.0
            
        try:
            embeddings1 = self.model.encode(text1, convert_to_tensor=True)
            embeddings2 = self.model.encode(text2, convert_to_tensor=True)
            
            # Use util.cos_sim for high-fidelity cosine similarity
            similarity = util.cos_sim(embeddings1, embeddings2)
            return float(similarity[0][0])
        except Exception as e:
            logger.error(f"Similarity calculation failed: {e}")
            return 0.0

    def calculate_batch_similarity(self, anchor: str, candidates: List[str]) -> List[float]:
        """Calculates similarity between an anchor and a list of candidate texts."""
        if not self.model or not anchor or not candidates:
            return [0.0] * len(candidates)
            
        try:
            anchor_emb = self.model.encode(anchor, convert_to_tensor=True)
            candidate_embs = self.model.encode(candidates, convert_to_tensor=True)
            
            similarities = util.cos_sim(anchor_emb, candidate_embs)
            return similarities[0].tolist()
        except Exception as e:
            logger.error(f"Batch similarity calculation failed: {e}")
            return [0.0] * len(candidates)
