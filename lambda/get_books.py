import requests
import json
import boto3
import os
import logging


# TODO env variables
S3_BUCKET = os.environ.get('S3_BUCKET')
DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE")

def lambda_handler(event, context):
    new_books = get_new_books()
    books_to_add = [book for book in new_books if not exists_in_mongo(book)]
    for book in books_to_add:
        book_details = get_book_details(book)  # los libros pueden tener varios autores
        upload_to_s3(book_details)


def get_new_books():
    r = requests.get("https://api.itbook.store/1.0/new")
    return r.json()["books"]


def exists_in_mongo(book):
    dynamo_client = boto3.client('dynamodb', region_name='us-east-1')
    
    isbn13 = book['isbn13']
    condition = 'isbn13 = :isbn13'
    attributes = {':isbn13': {'S': isbn13}}
    response = dynamo_client.scan(TableName=DYNAMO_TABLE, IndexName='isbn13', FilterExpression=condition, ExpressionAttributeValues=attributes)
    matching_books = response.get('Items', [])
    if len(matching_books) != 0:
        logging.info(f"Book {book['title']} already existst!")
        return True
    return False


def get_book_details(book):
    r = requests.get(f"https://api.itbook.store/1.0/books/{book['isbn13']}")
    return r.json()


def upload_to_s3(book):
    s3 = boto3.client("s3")
    authors = book["authors"].split(",")
    for author in authors:
        author = author.replace(' ', '-')
        key = f"{author}/{book['isbn13']}.json"

        if not os.path.exists(f"/tmp/{book['isbn13']}.json"):
            serialize_json(book)
            print(book["isbn13"])
        s3.upload_file(Filename=f"/tmp/{book['isbn13']}.json", Bucket=S3_BUCKET, Key=key)


def serialize_json(book):
    """Serialize the json in which we have all the metrics"""
    with open(f"/tmp/{book['isbn13']}.json", "w") as fp:
        json.dump(book, fp)


# def insert_in_mongo(book):
#     return


# if __name__ == "__main__":
#     handler(None, None)
