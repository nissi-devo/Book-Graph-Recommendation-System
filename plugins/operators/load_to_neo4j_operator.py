import neo4j
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from neo4j import GraphDatabase
import json
import os

from data_transformation.data_cleaning import preprocess_categories
from data_transformation.data_validation import is_valid_review, is_valid_book


class ConditionalLoadOperator(BaseOperator):

    @apply_defaults
    def __init__(self, aws_conn_id, bucket_name, data_categ, neo4j_conn_uri, neo4j_user, neo4j_password, *args,
                 **kwargs):
        super(ConditionalLoadOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.data_categ = data_categ
        self.neo4j_conn_uri = neo4j_conn_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password

    def execute(self, context):
        s3 = S3Hook(aws_conn_id=self.aws_conn_id)

        if self.data_categ == 'meta_books':
            json_files = context['ti'].xcom_pull(task_ids='extract_books', key=f'{self.data_categ}_json_files')
        else:
            json_files = context['ti'].xcom_pull(task_ids='extract_reviews', key=f'{self.data_categ}_json_files')

        driver = GraphDatabase.driver(self.neo4j_conn_uri, auth=(self.neo4j_user, self.neo4j_password))

        def process_book(session, book):

            # Create Book node
            #ON CREATE SET: If the node is newly created (i.e., it didn't exist before), it sets the properties title, price etc
            #ON MATCH SET: If the node already exists (i.e., it was matched), it updates the properties only if they are NULL or haven't been set yet
            #The COALESCE function ensures that existing properties aren't overwritten with values coming from thw new data (note: in some cases this behaviour might be desirable)
            try:
                session.run(
                """
                MERGE (b:Book {asin: $asin})
                ON CREATE SET b.title = $title, b.price = $price, b.imUrl = $imUrl
                ON MATCH SET b.title = COALESCE(b.title, $title), 
                             b.price = COALESCE(b.price, $price), 
                             b.imUrl = COALESCE(b.imUrl, $imUrl)
                """,
                asin=book['asin'], title=book.get('title', None), price=book.get('price', None),
                imUrl=book.get('imUrl', None)
                )

                #Create categories and sub-categories nodes and relationships
                if 'categories' in book:
                    main_category = preprocess_categories(book['categories'])['main_category']
                    sub_categories = preprocess_categories(book['categories'])['sub_categories']

                    #if there is a category
                    if main_category:
                        session.run(
                            """
                            MERGE (main:Category {name: $main_category})
                            WITH main
                            MATCH (b:Book {asin: $asin})
                            MERGE (b)-[:BELONGS_TO]->(main)
                            """,
                            main_category=main_category, asin=book['asin']
                        )
                        # if there is at least one sub_category Nb: there can be no sub-categories if there is no main category
                        if sub_categories:
                            session.run(
                                """
                                MATCH (main:Category {name: $main_category})
                                FOREACH (sub_category_name IN $sub_categories |
                                MERGE (sub:Category {name: sub_category_name})
                                MERGE (main)-[:HAS_SUB_CATEGORY]->(sub))
                                """,
                                main_category=main_category,
                                sub_categories=sub_categories
                            )

                # Create relationships (ALSO_BOUGHT) if 'related' exists and 'also_bought' is present
                if 'related' in book and 'also_bought' in book['related']:
                    for also_bought_asin in book['related']['also_bought']:
                        session.run(
                           """
                           MERGE (b1:Book {asin: $asin1}) 
                           MERGE (b2:Book {asin: $asin2})
                           MERGE (b1)-[:ALSO_BOUGHT]->(b2)
                           """,
                           asin1=book['asin'], asin2=also_bought_asin
                        )


                # Create relationships (BUY_AFTER_VIEWING)
                if 'related' in book and 'buy_after_viewing' in book['related']:
                    for buy_after_viewing_asin in book['related']['buy_after_viewing']:
                            session.run(
                            """
                            MERGE (b1:Book {asin: $asin1}) 
                            MERGE (b2:Book {asin: $asin2})
                            MERGE (b1)-[:BUY_AFTER_VIEWING]->(b2)
                            """,
                            asin1=book['asin'], asin2=buy_after_viewing_asin
                            )
                # Create relationships (ALSO_VIEWED)
                if 'related' in book and 'also_viewed' in book['related']:
                    for also_viewed_asin in book['related']['also_viewed']:
                            session.run(
                            """
                            MERGE (b1:Book {asin: $asin1}) 
                            MERGE (b2:Book {asin: $asin2})
                            MERGE (b1)-[:ALSO_VIEWED]->(b2)
                            """,
                            asin1=book['asin'], asin2=also_viewed_asin
                            )
            except neo4j.exceptions.CypherTypeError as e:
                print(f"CypherTypeError for book {book['asin']}: {e}")
            except Exception as e:
                print(f"Unexpected error for book {book['asin']}: {e}")

        def process_review(session, review):
            session.run(
                """
                MERGE (r:Reviewer {reviewerID: $reviewerID})
                ON CREATE SET r.reviewerName = $reviewerName

                MERGE (b:Book {asin: $asin})

                WITH r, b
                MERGE (r)-[rev:REVIEWED]->(b)
                ON CREATE SET rev.reviewText = $reviewText, rev.overall = $overall, 
                              rev.summary = $summary, rev.unixReviewTime = $unixReviewTime, 
                              rev.reviewTime = $reviewTime, rev.helpful = $helpful
                """,
                reviewerID=review['reviewerID'], reviewerName=review.get('reviewerName', None),
                asin=review['asin'], reviewText=review.get('reviewText', None), overall=review.get('overall', None),
                summary=review.get('summary', None), unixReviewTime=review.get('unixReviewTime', None),
                reviewTime=review.get('reviewTime', None), helpful=review.get('helpful', None)
            )  # Attempts to match reviewers and books and create them if they don't exist. Nb: certain propoerties such as "reviewName" are only set if a new node is being created for the first time.

        with driver.session() as session:
            for json_file in json_files:
                content = s3.read_key(json_file, bucket_name=self.bucket_name)
                json_objects = json.loads(content)
                if self.data_categ == 'meta_books':
                    for book in json_objects:
                        if not is_valid_book(book):
                            print(f"Skipping invalid book object: {book}")
                            continue
                        process_book(session, book)
                elif self.data_categ == 'review_books':
                    for review in json_objects:
                        # Validate JSON object before processing
                        if not is_valid_review(review):
                            print(f"Skipping invalid review object: {review}")
                            continue
                        process_review(session, review)
