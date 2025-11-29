import praw
from dotenv import load_dotenv
import os
import re
import json
import time
import random
from kafka import KafkaProducer
import uuid


load_dotenv()

# Reddit authentication
reddit = praw.Reddit(
    client_id=os.getenv('CLIENT_ID'),
    client_secret=os.getenv('CLIENT_SECRET'),
    user_agent="genz-stream-app2"
)

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def remove_urls(text):
    return re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)

def fetch_reddit_posts(subreddit, keyword, limit=50):
    cleaned_posts = []
    try:
        for submission in reddit.subreddit(subreddit).search(keyword, limit=limit):
            post_id = str(uuid.uuid4())
            cleaned_posts.append({
                "id": post_id,
                "title": remove_urls(submission.title),
                "content": remove_urls(submission.selftext)
            })
    except Exception as e:
        print(f"Error fetching posts: {e}")
    return cleaned_posts

def stream_to_kafka(subreddit, keyword, topic='reddit_stock_comments', interval=(1, 3)):
    print(f"Streaming posts from r/{subreddit} ...")
    try:
        while True:
            posts = fetch_reddit_posts(subreddit, keyword)
            for post in posts:
                producer.send(topic, post)
            producer.flush()
            print(f"Sent {len(posts)} posts")
            time.sleep(random.uniform(*interval))
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        producer.close()

if __name__ == "__main__":
    stream_to_kafka('WallStreetBets', 'NVDA')
