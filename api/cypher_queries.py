GET_REVIEWED_BOOKS_COUNT_WITHIN_TIMEFRAME = """
MATCH (:Reviewer)-[r:REVIEWED]->(b:Book)
WHERE r.unixReviewTime >= $startTime AND r.unixReviewTime <= $endTime
RETURN count(DISTINCT b) AS totalReviewedBooks
"""

GET_ALL_REVIEWED_BOOKS_COUNT = """
MATCH (:Reviewer)-[r:REVIEWED]->(b:Book)
RETURN count(DISTINCT b) AS totalReviewedBooks
"""

#Recommend Books Reviewed by Similar Reviewers
GET_BOOKS_REVIEWED_BY_SIMILAR_REVIEWERS = """
MATCH (b:Book {asin: $asin})<-[:REVIEWED]-(r:Reviewer)-[:REVIEWED]->(rec:Book)
RETURN rec, COUNT(*) AS reviewCount
ORDER BY reviewCount DESC
LIMIT 10
"""


