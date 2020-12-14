"""Metaflow pipeline to collect user timeline data from twitter."""
import logging
import json
import sys

import toolz.curried as t
import tweepy
from loguru import logger
from metaflow import (
    conda_base,
    step,
    FlowSpec,
    IncludeFile,
    JSONType,
    Parameter,
    S3,
)



logger.remove()
logger.add(
    sink=sys.stderr, filter={__name__: logging.INFO, "research_daps": logging.INFO}
)


@conda_base(
    libraries={
        "toolz": "0.11.0",
        "loguru": "0.5.3",
        "tqdm": "4.51.0",
        "tweepy": "3.9.0",
        "tenacity": "6.2.0",
    }
)
class TwitterTimeline(FlowSpec):
    handle_file = IncludeFile(
        "handle-file",
        help="Newline delimited list of handles to get",
    )

    test_mode = Parameter(
        "test_mode",
        help="Whether to run in test mode (fetch a subset of data)",
        type=bool,
        default=True,
    )

    api_parameters = Parameter(
        "api-parameters", help="Tweepy API parameters (JSON)", type=JSONType, default={}
    )

    consumer_key = Parameter(
        "consumer-key", help="Twitter API consumer key", type=str, required=True
    )

    consumer_secret = Parameter(
        "consumer-secret", help="Twitter API consumer secret", type=str, required=True
    )

    s3_location = Parameter(
        "s3-location", help="S3 key to store data in", type=str, required=True
    )

    @step
    def start(self):
        """Collect tweets using tweepy."""
        from tweepy_utils import user_timeline_tweets, handle_prefix

        handles = self.handle_file.split("\n")

        if self.test_mode:
            handles = list(t.take(4, handles))

        auth = tweepy.AppAuthHandler(self.consumer_key, self.consumer_secret)
        api = tweepy.API(auth)

        for handle in handles:
            logger.info(handle)
            tweets = user_timeline_tweets(api, handle, self.api_parameters)

            if tweets:
                # Save tweets
                with S3(s3root=self.s3_location) as s3:
                    s3.put(handle_prefix(handle), json.dumps(tweets))
                # Mark completion
                with S3(s3root=self.s3_location) as s3:
                    s3.put(f".completed/{handle}", "1")
                    logger.info(f"Completed {handle}")
            else:
                with S3(s3root=self.s3_location) as s3:
                    s3.put(f".failures/{handle}", "1")
                    logger.warning(f"Couldn't collect {handle}")

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    TwitterTimeline()
