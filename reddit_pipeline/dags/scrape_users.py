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
# Task 1 — scrape_users
# ---------------------------------------------------------------------------

def scrape_users() -> None:
    import RedScrapsLib as rs
    from shared.kafka_client import get_producer
    from shared.mongo_client import get_db

    rs.init(user_agent="BookScraperBot/1.0")
    producer = get_producer()
    db = get_db()

    pending = list(db.users.find({"scraped": False}, {"username": 1}))
    log.info("scrape_users: %d users pending", len(pending))

    published = 0
    for doc in pending:
        username = doc["username"]

        submitted = rs.get_user_posts(user=username)
        commented = rs.get_user_comments(user=username)

        if submitted is None and commented is None:
            log.warning("no data for user %s — marking scraped to avoid retry", username)
            db.users.update_one({"_id": username}, {"$set": {"scraped": True}})
            continue

        posts_list = []
        if submitted and submitted.Posts:
            for p in submitted.Posts:
                posts_list.append({
                    "post_id":       p.PostID,
                    "title":         p.Title,
                    "subreddit":     p.Subreddit,
                    "self_text":     p.SelfText,
                    "link":          p.Link,
                    "upvotes":       p.Upvotes,
                    "comment_count": p.CommentCount,
                    "created_utc":   p.CreatedUtc,
                })

        comments_list = []
        if commented and commented.Comments:
            for c in commented.Comments:
                comments_list.append({
                    "comment_id":  c.CommentID,
                    "subreddit":   c.Subreddit,
                    "body":        c.Body,
                    "parent_id":   c.ParentID,
                    "post_id":     c.PostID,
                    "post_title":  c.PostTitle,
                    "link":        c.Link,
                    "upvotes":     c.Upvotes,
                    "created_utc": c.CreatedUtc,
                })

        src = submitted or commented
        msg = {
            "username":    src.Username,
            "first_id":    src.FirstID,
            "last_id":     src.LastID,
            "total_count": src.TotalCount,
            "posts":       posts_list,
            "comments":    comments_list,
            "scraped_at":  _now_iso(),
        }
        producer.send("raw-users", value=msg)

        # Mark scraped so the next run skips this user
        db.users.update_one({"_id": username}, {"$set": {"scraped": True}})
        published += 1

    producer.flush()
    log.info("scrape_users: published %d users to raw-users", published)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="scrape_users",
    schedule="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0},
) as dag:

    PythonOperator(task_id="scrape_users", python_callable=scrape_users)
