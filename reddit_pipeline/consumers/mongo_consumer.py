"""Persistent consumer: dedupes via Redis, writes to MongoDB."""
import logging
import os
import sys

sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")))

from shared.kafka_client import get_consumer
from shared.mongo_client import get_db
from shared.redis_client import is_seen, mark_seen

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPICS = ["raw-posts", "raw-comments", "raw-users"]
GROUP_ID = "mongo-writer"


def _handle_post(db, msg: dict) -> None:
    post_id = msg["post_id"]
    if is_seen("posts", post_id):
        log.debug("skip duplicate post %s", post_id)
        return
    doc = {**msg, "_id": post_id}
    db.posts.insert_one(doc)
    mark_seen("posts", post_id)
    log.info("inserted post %s", post_id)


def _handle_comments(db, msg: dict) -> None:
    post_id = msg["post_id"]
    if is_seen("comments", post_id):
        log.debug("skip duplicate comments for post %s", post_id)
        return
    db.posts.update_one(
        {"_id": post_id},
        {"$set": {"comments": msg["comments"]}},
    )
    mark_seen("comments", post_id)
    log.info("embedded comments for post %s", post_id)


def _handle_user(db, msg: dict) -> None:
    username = msg["username"]
    if is_seen("users", username):
        log.debug("skip duplicate user %s", username)
        return
    doc = {**msg, "_id": username}
    db.users.insert_one(doc)
    mark_seen("users", username)
    log.info("inserted user %s", username)


def _ensure_indexes(db) -> None:
    db.posts.create_index("post_id", unique=True)
    db.posts.create_index("author")
    db.posts.create_index("comments_scraped")
    db.posts.create_index("scraped_at")
    db.users.create_index("username", unique=True)
    db.users.create_index("total_count")


def run() -> None:
    db = get_db()
    _ensure_indexes(db)
    consumer = get_consumer(TOPICS, GROUP_ID)
    log.info("mongo consumer started, subscribed to %s", TOPICS)

    for kafka_msg in consumer:
        topic = kafka_msg.topic
        msg = kafka_msg.value
        try:
            if topic == "raw-posts":
                _handle_post(db, msg)
            elif topic == "raw-comments":
                _handle_comments(db, msg)
            elif topic == "raw-users":
                _handle_user(db, msg)
        except Exception:
            log.exception("error processing message from %s: %s", topic, msg)


if __name__ == "__main__":
    run()
