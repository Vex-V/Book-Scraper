"""Persistent consumer: indexes documents into Elasticsearch."""
import logging
import os
import sys

sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")))

from elasticsearch import Elasticsearch

from shared.kafka_client import get_consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ES_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
TOPICS = ["raw-posts", "raw-comments", "raw-users"]
GROUP_ID = "elasticsearch-syncer"

POSTS_MAPPING = {
    "mappings": {
        "properties": {
            "title":     {"type": "text",    "analyzer": "standard"},
            "self_text": {"type": "text",    "analyzer": "standard"},
            "author":    {"type": "keyword"},
            "subreddit": {"type": "keyword"},
        }
    }
}

USERS_MAPPING = {
    "mappings": {
        "properties": {
            "username": {"type": "keyword"},
            "comments": {
                "type": "nested",
                "properties": {
                    "body": {"type": "text", "analyzer": "standard"},
                },
            },
            "posts": {
                "type": "nested",
                "properties": {
                    "self_text": {"type": "text", "analyzer": "standard"},
                },
            },
        }
    }
}


def _ensure_indexes(es: Elasticsearch) -> None:
    if not es.indices.exists(index="posts"):
        es.indices.create(index="posts", body=POSTS_MAPPING)
        log.info("created ES index: posts")
    if not es.indices.exists(index="users"):
        es.indices.create(index="users", body=USERS_MAPPING)
        log.info("created ES index: users")


def _handle_post(es: Elasticsearch, msg: dict) -> None:
    es.index(index="posts", id=msg["post_id"], document=msg)
    log.info("indexed post %s", msg["post_id"])


def _handle_comments(es: Elasticsearch, msg: dict) -> None:
    post_id = msg["post_id"]
    es.update(
        index="posts",
        id=post_id,
        body={"doc": {"comments": msg["comments"]}},
    )
    log.info("updated post %s with comments in ES", post_id)


def _handle_user(es: Elasticsearch, msg: dict) -> None:
    es.index(index="users", id=msg["username"], document=msg)
    log.info("indexed user %s", msg["username"])


def run() -> None:
    es = Elasticsearch(ES_URL)
    _ensure_indexes(es)
    consumer = get_consumer(TOPICS, GROUP_ID)
    log.info("elasticsearch consumer started, subscribed to %s", TOPICS)

    for kafka_msg in consumer:
        topic = kafka_msg.topic
        msg = kafka_msg.value
        try:
            if topic == "raw-posts":
                _handle_post(es, msg)
            elif topic == "raw-comments":
                _handle_comments(es, msg)
            elif topic == "raw-users":
                _handle_user(es, msg)
        except Exception:
            log.exception("error processing message from %s: %s", topic, msg)


if __name__ == "__main__":
    run()
