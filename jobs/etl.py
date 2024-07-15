import sys
import os

import neo4j

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import boto3
from dotenv import load_dotenv
from neo4j import GraphDatabase
#User-defined packages
from data_transformation.data_cleaning import preprocess_categories
from data_transformation.data_validation import is_valid_book, is_valid_review


def extract_from_s3(bucket_name, data_categ, year):

    prefix = f"{data_categ}/{year}/"
    continuation_token = None
    json_files = []

    while True:
        list_params = {
            'Bucket': bucket_name,
            'Prefix': prefix
        }
        #If there is more data to retrieve as a result of a previous trucncation provide that location so the data is retrieved
        if continuation_token:
            list_params['ContinuationToken'] = continuation_token

        #Get the all objects contained in a given bucket directory example meta_books/2024/
        response = s3.list_objects_v2(**list_params)

        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.json'): #Ensure you only extract JSON files
                    json_files.append(obj['Key'])

        if response['IsTruncated']: #If IsTruncated is True, it means there are more objects to retrieve
            continuation_token = response['NextContinuationToken']
        else:
            break

    return json_files
def generate_json_objects(data_categ):
    json_files = extract_from_s3(bucket_name, data_categ, '2024')
    for json_file in json_files:
        response = s3.get_object(Bucket=bucket_name, Key=json_file)
        content = response['Body'].read().decode('utf-8')
        json_objects = json.loads(content)

        for json_obj in json_objects:
            yield json_obj

def transform_review_data():
    pass
def load_to_neo4j(session, data_categ):
    #Set unique constraint on 'asin'. Uncomment and run once.
    #session.run("CREATE CONSTRAINT FOR (b:Book) REQUIRE b.asin IS UNIQUE")

    #Deal with the book metadata
    if data_categ == 'meta_books':
        for book in generate_json_objects(data_categ):
            if not is_valid_book(book):
                print(f"Skipping invalid book object: {book}")
                continue
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


    # Deal with the book reviews
    if data_categ == 'review_books':
        for review in generate_json_objects(data_categ):
            # Validate JSON object before processing
            if not is_valid_review(review):
                print(f"Skipping invalid review object: {review}")
                continue
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
            ) #Attempts to match reviewers and books and create them if they don't exist. Nb: certain propoerties such as "reviewName" are only set if a new node is being created for the first time.


if __name__ == "__main__":
    load_dotenv()

    # Connect to S3 bucket
    s3 = boto3.client('s3',
                      aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
                      aws_secret_access_key=os.getenv('AWS_SECRET'),
                      region_name='us-east-1'

                      )
    bucket_name = 'book-reviews'

    # Initialize the Neo4j driver
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = os.getenv("NEO4J_USER")
    neo4j_password = os.getenv("NEO4J_PASSWORD")
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    with driver.session() as session:
        load_to_neo4j(session, 'meta_books')
        load_to_neo4j(session, 'review_books')

