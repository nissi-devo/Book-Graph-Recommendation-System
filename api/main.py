import os
from typing import Optional

from fastapi import FastAPI, Query, HTTPException

from models.schemas import RecommendationsResponse
from services.neo4j_service import Neo4jService
from helpers.utils import date_to_unix_time


app = FastAPI()
# Initialize the Neo4j driver. Use 'neo4j' as the hostname, which is the service name in docker-compose.yml
neo4j_uri = "bolt://neo4j:7687"
neo4j_user = os.getenv("NEO4J_USER")
neo4j_password = os.getenv("NEO4J_PASSWORD")

# Create an instance of Neo4jService
neo4j_service = Neo4jService(neo4j_uri, neo4j_user, neo4j_password)

@app.get("/reviewed-books/count")
def get_reviewed_books_count(
        start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
        end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format")
):
    if start_date and end_date:
        start_time_unix = date_to_unix_time(start_date)
        end_time_unix = date_to_unix_time(end_date) + 86399  # Adjust to end of day (23:59:59)

        record = neo4j_service.get_reviewed_books_count(start_time_unix, end_time_unix)
    else:
        record = neo4j_service.get_reviewed_books_count()

    if record is None or "totalReviewedBooks" not in record.keys():
        raise HTTPException(status_code=404, detail="No reviewed books found within the specified time frame")

    totalReviewedBooks = record["totalReviewedBooks"]

    return {"totalReviewedBooks": totalReviewedBooks}

@app.get("/recommendations/similar-reviewers", response_model=RecommendationsResponse)
def recommend_books_by_similar_reviewers(asin: str):
    recommendations = neo4j_service.recommend_books_by_similar_reviewers(asin)
    if not recommendations:
        raise HTTPException(status_code=404, detail="No recommendations found")
    return {"recommendations": recommendations}