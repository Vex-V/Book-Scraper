import RedScrapsLib as rs

rs.init(user_agent="MyBot/1.0")

# Subreddit posts
posts = rs.get_home("python", limit=10)
for post in posts.Posts:
    print(post.Title, post.Author)
print("\n")
print("#####################")


# Post comments
comments = rs.get_comments("python", post_id="1sek3gq", limit=50)
for comment in comments.Comments:
    print(comment.Body, comment.Author)
print("\n")
print("#####################")


# User activity
submissions = rs.get_user_posts("spez", limit=25)
for submission in submissions.Posts:
    print(submission.Title, submission.Subreddit)


print("\n")
print("#####################")


user_comments = rs.get_user_comments("spez", limit=25)
for comment in user_comments.Comments:
    print(comment.Body, comment.Subreddit)


print("\n")
print("#####################")