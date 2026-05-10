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


# ---------------------------------------------------------------------------
# Task 1 — scrape_posts
# ---------------------------------------------------------------------------

def scrape_posts() -> None:
    import RedScrapsLib as rs
    from shared.kafka_client import get_producer
    from shared.mongo_client import get_db

    rs.init(user_agent="BookScraperBot/1.0")
    producer = get_producer()
    db = get_db()

    state = db.scrape_state.find_one({"_id": "books_after"})
    after = state["last_id"] if state else None
    log.info("Task 1: fetching posts after=%s", after)

    home = rs.get_home(subreddit="books", after=after)
    if home is None or not home.Posts:
        log.warning("get_home returned no posts — resetting after cursor")
        db.scrape_state.delete_one({"_id": "books_after"})
        return

    existing_ids = {
        doc["post_id"]
        for doc in db.posts.find({}, {"post_id": 1})
    }

    published = 0
    for post in home.Posts:
        post_id = post.PostID
        if not post_id or post_id in existing_ids:
            continue

        msg = {
            "post_id":          post_id,
            "title":            post.Title,
            "author":           post.Author,
            "self_text":        post.SelfText,
            "link":             post.Link,
            "subreddit":        "books",
            "scraped_at":       _now_iso(),
            "comments_scraped": False,
        }
        producer.send("raw-posts", value=msg)
        published += 1

    producer.flush()

    db.scrape_state.update_one(
        {"_id": "books_after"},
        {"$set": {"last_id": home.LastID}},
        upsert=True,
    )
    log.info("Task 1: published %d new posts, cursor advanced to %s", published, home.LastID)


# ---------------------------------------------------------------------------
# Task 2 — scrape_comments
# ---------------------------------------------------------------------------

def scrape_comments() -> None:
    import RedScrapsLib as rs
    from shared.kafka_client import get_producer
    from shared.mongo_client import get_db

    rs.init(user_agent="BookScraperBot/1.0")
    producer = get_producer()
    db = get_db()

    pending = list(db.posts.find({"comments_scraped": False}, {"post_id": 1}))
    log.info("Task 2: %d posts pending comment scrape", len(pending))

    for doc in pending:
        post_id = doc["post_id"]
        result = rs.get_comments(subreddit="books", post_id=post_id)
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

        msg = {"post_id": post_id, "comments": comments}
        producer.send("raw-comments", value=msg)
        producer.flush()

        # Insert new comment authors as unscrapped user stubs
        for username in authors:
            db.users.update_one(
                {"_id": username},
                {"$setOnInsert": {"username": username, "scraped": False}},
                upsert=True,
            )

        db.posts.update_one({"_id": post_id}, {"$set": {"comments_scraped": True}})
        log.info("Task 2: scraped %d comments for post %s, queued %d new authors",
                 len(comments), post_id, len(authors))


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="scrape_posts_comments",
    schedule="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0},
) as dag:

    t1 = PythonOperator(task_id="scrape_posts",    python_callable=scrape_posts)
    t2 = PythonOperator(task_id="scrape_comments", python_callable=scrape_comments)

    t1 >> t2
