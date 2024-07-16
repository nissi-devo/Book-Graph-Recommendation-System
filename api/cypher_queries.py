GET_REVIEWED_BOOKS_COUNT_WITHIN_TIMEFRAME = """
MATCH (:Reviewer)-[r:REVIEWED]->(b:Book)
WHERE r.unixReviewTime >= $startTime AND r.unixReviewTime <= $endTime
RETURN count(DISTINCT b) AS totalReviewedBooks
"""