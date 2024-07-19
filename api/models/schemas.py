from pydantic import BaseModel
from typing import List, Dict

class Book(BaseModel):
    identity: int
    labels: List[str]
    properties: Dict[str, str]
    elementId: str

class Recommendation(BaseModel):
    recommendedBook: Book
    reviewCount: int

class RecommendationsResponse(BaseModel):
    recommendations: List[Recommendation]
