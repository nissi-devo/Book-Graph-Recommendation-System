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

#Recommend Books Frequently Bought Together
GET_BOOKS_ALSO_BOUGHT = """
MATCH (b:Book {asin: $asin})-[:ALSO_BOUGHT]->(rec:Book)-[:BELONGS_TO]->(c:Category)-[:HAS_SUB_CATEGORY]->(sc:Category)
WITH rec, c, collect(sc.name) AS subcategories
RETURN rec.asin AS asin, rec.title AS title, rec.price AS price, c.name AS category, subcategories
"""

#Recommend books with helpful votes
GET_BOOKS_WITH_HELPFUL_VOTES = """
MATCH (b:Book {asin: $asin})<-[rev:REVIEWED]-(r:Reviewer)
WITH rev, r, b, rev.helpful[0] AS helpfulVotes, rev.helpful[1] AS totalVotes
RETURN r.reviewerName AS reviewer, rev.reviewText AS review, helpfulVotes, totalVotes,
       (CASE WHEN totalVotes = 0 THEN 0 ELSE (1.0 * helpfulVotes / totalVotes) END) AS helpfulness
ORDER BY helpfulness DESC, totalVotes DESC
LIMIT 10
"""
#Recommend Books belonging to same category
GET_BOOKS_IN_SAME_CATEGORY = """
MATCH (b:Book {asin: $asin})-[:BELONGS_TO]->(c:Category)<-[:BELONGS_TO]-(rec:Book)
RETURN rec, c.name as category
LIMIT 10
"""

