import logging
import os
import sys
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add project root (parent of dags/) to sys.path so shared/ is importable
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
        if result.Comments:
            for c in result.Comments:
                comments.append({
                    "comment_id": c.CommentID,
                    "author":     c.Author,
                    "parent_id":  c.ParentID,
                    "body":       c.Body,
                    "scraped_at": _now_iso(),
                })

        msg = {"post_id": post_id, "comments": comments}
        producer.send("raw-comments", value=msg)
        producer.flush()

        # Only direct MongoDB write allowed in DAG tasks: flip the state flag
        db.posts.update_one({"_id": post_id}, {"$set": {"comments_scraped": True}})
        log.info("Task 2: scraped %d comments for post %s", len(comments), post_id)


# ---------------------------------------------------------------------------
# Task 3 — scrape_users
# ---------------------------------------------------------------------------

def scrape_users() -> None:
    import RedScrapsLib as rs
    from shared.kafka_client import get_producer
    from shared.mongo_client import get_db

    rs.init(user_agent="BookScraperBot/1.0")
    producer = get_producer()
    db = get_db()

    # Distinct authors from posts whose comments have been scraped
    comment_authors = set(
        db.posts.distinct("comments.author", {"comments_scraped": True})
    )
    # Authors already in the users collection
    known_users = {
        doc["username"]
        for doc in db.users.find({}, {"username": 1})
    }

    new_authors = comment_authors - known_users - {None, ""}
    log.info("Task 3: %d new authors to scrape", len(new_authors))

    published = 0
    for username in new_authors:
        submitted = rs.get_user_posts(user=username)
        commented = rs.get_user_comments(user=username)

        if submitted is None and commented is None:
            log.warning("no data for user %s, skipping", username)
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
                    "comment_id": c.CommentID,
                    "subreddit":  c.Subreddit,
                    "body":       c.Body,
                    "parent_id":  c.ParentID,
                    "post_id":    c.PostID,
                    "post_title": c.PostTitle,
                    "link":       c.Link,
                    "upvotes":    c.Upvotes,
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
        published += 1

    producer.flush()
    log.info("Task 3: published %d users to raw-users", published)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="scrape_books_pipeline",
    schedule="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0},
) as dag:

    t1 = PythonOperator(task_id="scrape_posts",    python_callable=scrape_posts)
    t2 = PythonOperator(task_id="scrape_comments", python_callable=scrape_comments)
    t3 = PythonOperator(task_id="scrape_users",    python_callable=scrape_users)

    t1 >> t2 >> t3
