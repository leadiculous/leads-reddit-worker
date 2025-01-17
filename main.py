import json
import logging
import time
import grpc
import asyncio
import asyncpg
import asyncpraw
from os import getenv
from dotenv import load_dotenv
from praw.models import Submission, Redditor
from ai_service_pb2 import ClassificationRequest, ClassificationResponse
from ai_service_pb2_grpc import ClassifierStub
from google.protobuf.json_format import MessageToJson
from supabase import acreate_client, AClient, AClientOptions

load_dotenv()

if getenv("DEBUG_ALL").lower() == "true":
    logging.basicConfig(level=logging.DEBUG)


class RedditWorker:
    db: asyncpg.connection.Connection
    campaigns: list

    def __init__(self):
        self.logger = self.create_logger(getenv("DEBUG").lower() == "true")
        self.logger.setLevel(logging.DEBUG if getenv("DEBUG").lower() == "true" else logging.WARNING)

    @staticmethod
    def create_logger(debug: bool):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG if debug else logging.WARNING)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        return logger

    async def init(self):
        self.db = await asyncpg.connect(getenv("DATABASE_URL"))
        # enable automatic JSON type conversion
        await self.db.set_type_codec(
            'json',
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog"
        )
        self.campaigns = await self._load_campaigns()
        self.logger.info(f"loaded {len(self.campaigns)} initial campaigns")
        self.log_campaigns()

    def log_campaigns(self):
        # JSON and to string methods don't work on the Record type for some reason, so I'm doing it manually:
        self.logger.debug("--campaigns--")
        for campaign in self.campaigns:
            self.logger.debug(f"campaign id: {campaign['campaign_id']}")
            self.logger.debug("tags:")
            tags = campaign['tags']
            for tag in tags:
                self.logger.debug(f"\t id: {tag['id']}, tag: {tag['tag']}")
        self.logger.debug("-" * 13)

    async def _load_campaigns(self):
        return await self.db.fetch(
            """
              SELECT 
                c.id as campaign_id, 
                c.user_id,
                COALESCE(
                  json_agg(json_build_object('id', t.id, 'tag', t.tag)) FILTER (WHERE t.id IS NOT NULL),
                  '[]'
                ) AS tags 
              FROM public.campaigns c
              LEFT JOIN public.campaign_tags t ON t.campaign_id = c.id
              GROUP BY c.id
            """
        )

    def on_campaign_change(self, payload):
        data = payload['data']
        table = data['table']
        event_type = data['type']

        if table == "campaign_tags":
            for i, campaign in enumerate(self.campaigns):
                if event_type == "INSERT":
                    # the inserted record
                    record = data['record']

                    # add the tag to the campaign
                    if campaign['campaign_id'] == record['campaign_id']:
                        self.campaigns[i]['tags'].append({"id": record['id'], "tag": record['tag']})
                        break

                elif event_type == "DELETE":
                    # the id of the deleted tag
                    tag_id = data['old_record']['id']

                    # find and remove the tag from the campaign
                    for tag in self.campaigns[i]['tags']:
                        if tag['id'] == tag_id:
                            self.campaigns[i]['tags'].remove(tag)
                            break
        elif table == "campaigns":
            if event_type == "INSERT":
                # the inserted record
                record = data['record']

                # add the campaign to the list
                self.campaigns.append({
                    "campaign_id": record['id'],
                    "user_id": record['user_id'],
                    "tags": []
                })
            elif event_type == "DELETE":
                # the id of the deleted campaign
                campaign_id = data['old_record']['id']

                # remove the campaign from the list
                for i, campaign in enumerate(self.campaigns):
                    if campaign['campaign_id'] == campaign_id:
                        self.campaigns.pop(i)
                        break

        self.log_campaigns()

    async def monitor_campaigns(self):
        supabase: AClient = await acreate_client(
            getenv("SUPABASE_URL"),
            getenv("SUPABASE_KEY"),
            options=AClientOptions(realtime={"auto_reconnect": True, "max_retries": 10})
        )

        await supabase.realtime.connect()

        for table in ["campaigns", "campaign_tags"]:
            await (supabase.realtime
                   .channel(f"reddit_worker_{table}")
                   .on_postgres_changes("*", schema="public", table=table, callback=self.on_campaign_change)
                   .subscribe())

        await supabase.realtime.listen()

    async def monitor_subreddits(self):
        # create praw instance
        reddit = asyncpraw.Reddit(
            client_id=getenv("CLIENT_ID"),
            client_secret=getenv("CLIENT_SECRET"),
            username=getenv("DEFAULT_USERNAME"),
            password=getenv("DEFAULT_PASSWORD"),
            user_agent="Leads Finder App (by u/Sleeyax1)",
        )

        # create AI service
        ai_service = ClassifierStub(grpc.aio.insecure_channel(getenv("AI_SERVICE_ADDRESS")))

        subreddit = await reddit.subreddit("all")
        async for submission in subreddit.stream.submissions():
            for campaign in self.campaigns:
                campaign_id = campaign['campaign_id']
                user_id = campaign['user_id']
                topics = [tag['tag'] for tag in campaign['tags']]

                # the AI service can't process data without topics
                if len(topics) == 0:
                    continue

                req = ClassificationRequest(title=submission.title, body=submission.selftext)
                req.topics.extend(topics)

                res: ClassificationResponse = await ai_service.Classify(req)

                if len(res.matched_topics) > 0:
                    print(f"Reddit post URL: https://www.reddit.com{submission.permalink}")
                    print("AI request: ")
                    print(MessageToJson(req))
                    print("AI response: ")
                    print(MessageToJson(res))
                    print("-" * 50)
                    start = time.time()
                    await self.save_submission(campaign_id, user_id, submission, res)
                    end = time.time()
                    self.logger.debug(f"Saved submission in {end - start} seconds")

    async def save_submission(self, campaign_id: int, user_id: str, submission: Submission, classification: ClassificationResponse):
        async with self.db.transaction():
            # write the lead to the database
            query = """
                INSERT INTO leads (
                    campaign_id, 
                    user_id,
                    confidence_score_threshold,
                    post_source, 
                    post_author, 
                    post_created_at, 
                    post_is_nsfw,  
                    post_url, 
                    post_title, 
                    post_content
                ) VALUES ($1, $2, $3, $4, $5, to_timestamp($6), $7, $8, $9, $10) 
                RETURNING id
            """
            lead_id = await self.db.fetchval(
                query,
                campaign_id,
                user_id,
                classification.score_threshold,
                "reddit",
                submission.author.name,
                submission.created_utc,
                submission.over_18,
                f"https://www.reddit.com{submission.permalink}",
                submission.title,
                submission.selftext
            )

            # write the classification results to the database
            for topic in classification.matched_topics:
                query = """
                    INSERT INTO lead_topics (
                        lead_id,
                        topic,
                        confidence_score
                    ) VALUES ($1, $2, $3)
                """
                await self.db.execute(
                    query,
                    lead_id,
                    topic.label,
                    topic.score
                )

    async def start(self):
        await asyncio.gather(
            self.monitor_campaigns(),
            self.monitor_subreddits(),
        )


async def main():
    worker = RedditWorker()
    await worker.init()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
