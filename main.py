import threading
import grpc
import praw
import psycopg2
from os import getenv
from dotenv import load_dotenv
from ai_service_pb2 import ClassificationRequest, ClassificationResponse
from ai_service_pb2_grpc import ClassifierStub
from google.protobuf.json_format import MessageToJson

load_dotenv()

db = psycopg2.connect(getenv("DATABASE_URL"))
cursor = db.cursor()


def main():
    # create praw instance
    reddit = praw.Reddit(
        client_id=getenv("CLIENT_ID"),
        client_secret=getenv("CLIENT_SECRET"),
        username=getenv("DEFAULT_USERNAME"),
        password=getenv("DEFAULT_PASSWORD"),
        user_agent="Leads Finder App (by u/Sleeyax1)",
    )

    # create AI service
    ai_service = ClassifierStub(grpc.insecure_channel(getenv("AI_SERVICE_ADDRESS")))

    # fetch the initial campaign data
    cursor.execute("""
    SELECT t.campaign_id, array_agg(t.tag) as tags FROM public.campaigns c
    INNER JOIN public.campaign_tags t ON t.campaign_id = c.id
    GROUP BY t.campaign_id
    """)
    campaigns = cursor.fetchall()

    print(f"Loaded {len(campaigns)} initial campaigns")

    # TODO: update campaigns in-memory using supabase real-time events

    # monitor subreddits
    subreddit = reddit.subreddit("all")
    for submission in subreddit.stream.submissions():
        for campaign in campaigns:
            campaign_id = campaign[0]
            topics = campaign[1]

            req = ClassificationRequest(title=submission.title, body=submission.selftext)
            req.topics.extend(topics)

            res: ClassificationResponse = ai_service.Classify(req)

            if len(res.matched_topics) > 0:
                print(f"Reddit post URL: {submission.url}")
                print("AI request: ")
                print(MessageToJson(req))
                print("AI response: ")
                print(MessageToJson(res))
                print("-" * 50)


if __name__ == "__main__":
    main()
