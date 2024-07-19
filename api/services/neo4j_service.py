import os
import sys
from neo4j import GraphDatabase

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#User-defined packages
import cypher_queries


class Neo4jService:
    def __init__(self, neo4j_uri,neo4j_user,neo4j_password):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    def get_reviewed_books_count(self, start_time_unix=None, end_time_unix=None):
        if start_time_unix is not None and end_time_unix is not None:
            # Method with parameters to fetch reviewed books within a time frame
            with self.driver.session() as session:
                result = session.run(
                    cypher_queries.GET_REVIEWED_BOOKS_COUNT_WITHIN_TIMEFRAME,
                    startTime=start_time_unix, endTime=end_time_unix
                )


                return result.single()
        else:
            # Method without parameters to fetch count of all reviewed books
            with self.driver.session() as session:
                result = session.run(
                    cypher_queries.GET_ALL_REVIEWED_BOOKS_COUNT
                )
                return result.single()  # Return count of all reviewed books

    def recommend_books_by_similar_reviewers(self, asin):
        with self.driver.session() as session:
            result = session.run(cypher_queries.GET_BOOKS_REVIEWED_BY_SIMILAR_REVIEWERS, asin=asin)

            recommendations = []
            for record in result:
                recommended_book = record["rec"]
                recommendation = {
                    "recommendedBook": {
                        "identity": recommended_book.id,
                        "labels": list(recommended_book.labels),
                        "properties": dict(recommended_book.items()),
                        "elementId": recommended_book.element_id
                    },
                    "reviewCount": record["reviewCount"]
                }
                recommendations.append(recommendation)

            return recommendations
