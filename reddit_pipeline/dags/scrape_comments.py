import logging
import os
import sys
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

_pipeline_root = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _pipeline_root not in sys.path:
    sys.path.insert(0, _pipeline_root)

log = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def scrape_comments() -> None:
    import RedScrapsLib as rs
    from shared.es_client import get_es
    from shared.mongo_client import get_db
    from shared.redis_client import is_seen, mark_seen

    rs.init(user_agent="BookScraperBot/1.0")
    db = get_db()
    es = get_es()

    pending = list(db.posts.find({"comments_scraped": False}, {"post_id": 1, "subreddit": 1}))
    log.info("scrape_comments: %d posts pending", len(pending))

    for doc in pending:
        post_id = doc["post_id"]
        subreddit = doc.get("subreddit", "")

        if is_seen("comments", post_id):
            db.posts.update_one({"_id": post_id}, {"$set": {"comments_scraped": True}})
            continue

        result = rs.get_comments(subreddit=subreddit, post_id=post_id)
        if result is None:
            log.warning("get_comments returned None for post %s", post_id)
            continue

        comments = []
        authors = set()
        if result.Comments:
            for c in result.Comments:
                comments.append({
                    "comment_id": c.CommentID,
                    "author":     c.Author,
                    "parent_id":  c.ParentID,
                    "body":       c.Body,
                    "scraped_at": _now_iso(),
                })
                if c.Author and c.Author != "[deleted]":
                    authors.add(c.Author)

        db.posts.update_one(
            {"_id": post_id},
            {"$set": {"comments": comments, "comments_scraped": True}},
        )
        es.update(index="posts", id=post_id, body={"doc": {"comments": comments}})
        mark_seen("comments", post_id)

        for username in authors:
            db.users.update_one(
                {"_id": username},
                {"$setOnInsert": {"username": username, "scraped": False}},
                upsert=True,
            )

        log.info("post %s: %d comments, %d new authors queued", post_id, len(comments), len(authors))


with DAG(
    dag_id="scrape_comments",
    schedule="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0},
) as dag:
    PythonOperator(task_id="scrape_comments", python_callable=scrape_comments)
