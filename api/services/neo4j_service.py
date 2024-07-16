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

    def get_reviewed_books(self, start_time_unix, end_time_unix):
        with self.driver.session() as session:
            result = session.run(
                cypher_queries.GET_REVIEWED_BOOKS_COUNT_WITHIN_TIMEFRAME,
                startTime=start_time_unix, endTime=end_time_unix
            )

            return result.single()