import os

from elasticsearch import Elasticsearch

ES_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")

_POSTS_MAPPING = {
    "mappings": {
        "properties": {
            "title":     {"type": "text",    "analyzer": "standard"},
            "self_text": {"type": "text",    "analyzer": "standard"},
            "author":    {"type": "keyword"},
            "subreddit": {"type": "keyword"},
        }
    }
}

_USERS_MAPPING = {
    "mappings": {
        "properties": {
            "username": {"type": "keyword"},
            "comments": {
                "type": "nested",
                "properties": {
                    "body": {"type": "text", "analyzer": "standard"},
                },
            },
        }
    }
}


def get_es() -> Elasticsearch:
    return Elasticsearch(ES_URL)


def ensure_indices(es: Elasticsearch) -> None:
    if not es.indices.exists(index="posts"):
        es.indices.create(index="posts", body=_POSTS_MAPPING)
    if not es.indices.exists(index="users"):
        es.indices.create(index="users", body=_USERS_MAPPING)
