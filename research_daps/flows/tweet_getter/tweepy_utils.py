import time
from datetime import datetime
from typing import List, Optional, Generator

import tqdm
import tweepy
import toolz.curried as t
from loguru import logger
from tenacity import retry, wait_fixed, stop_after_attempt


def seconds_until_reset(
    rate_limit_status: dict, key=("statuses", "/statuses/user_timeline")
) -> float:
    """Calculate seconds until ratelimit reset for endpoint in `key`."""
    return abs(
        (
            datetime.fromtimestamp(
                t.get_in(key, rate_limit_status["resources"])["reset"]
            )
            - datetime.now()
        ).total_seconds()
    )


def queries_left(
    rate_limit_status: dict, key=("statuses", "/statuses/user_timeline")
) -> int:
    """Calculate number of queries left for endpoint in `key`."""
    return t.get_in(key, rate_limit_status["resources"])["remaining"]


def limit_handled(
    api: tweepy.API, cursor: tweepy.Cursor
) -> Generator[tweepy.Status, None, None]:
    """Yield items from `cursor` obeying Twitter ratelimit."""
    while True:
        try:
            yield cursor.next()
        except tweepy.TweepError as exc:
            if "429" in exc.args[0]:
                handle_ratelimit_wait(api)
                continue
            else:
                raise exc
        except StopIteration:
            return


def handle_ratelimit_wait(api: tweepy.API) -> None:
    """Handle's ratelimit waiting after 429 error from tweepy."""
    rate_limit_status = api.rate_limit_status()

    queries_exhausted = queries_left(rate_limit_status) == 0
    if queries_exhausted:
        seconds_to_wait = seconds_until_reset(rate_limit_status)
        logger.info(f"sleeping for {seconds_to_wait} seconds")
        time.sleep(seconds_to_wait)
    else:
        # Must have hit daily limit
        # Wait for a while to get more queries (assumes rolling 24 hour limit)
        daily_limit_sleep_time = 15 * 60
        logger.info("Received 429 with queries remaining. Daily limit hit?")
        logger.info(f"Sleeping for {daily_limit_sleep_time / 60} minutes")
        time.sleep(daily_limit_sleep_time)


def report_remaining_queries(d: dict, header: Optional[str] = None) -> None:
    """Report remaining API queries based on data `d`."""
    if header is None:
        report_remaining_queries(d["resources"], "entrypoint")
        return
    for k, v in d.items():
        if "limit" in d.keys():
            used = d["limit"] - d["remaining"]
            if used > 0:
                when = datetime.fromtimestamp(d["reset"])
                print(
                    f"{header}: {d['remaining']}/{d['limit']} remaining ({used} used) until {when}"
                )
            return
        elif isinstance(d, dict):
            report_remaining_queries(v, k)
        else:
            print("ret")
            return


def handle_prefix(handle: str) -> str:
    """Take up to the first two characters of `handle` and build folder prefix."""
    if len(handle) == 1:
        return f"{handle[0]}/{handle}"
    else:
        return f"{handle[0]}/{handle[1]}/{handle}"


@retry(wait=wait_fixed(5), stop=stop_after_attempt(3))
def user_timeline_tweets(
    api: tweepy.API, handle: str, api_parameters: dict
) -> Optional[List[dict]]:
    """Collect the tweets for account `handle`."""
    try:
        return [
            tweet._json
            for tweet in tqdm.tqdm(
                limit_handled(
                    api,
                    tweepy.Cursor(
                        api.user_timeline, id=handle, **api_parameters
                    ).items(),
                )
            )
        ]
    except tweepy.TweepError as e:
        if "404" in e.args[0]:  # Account not found
            return None
        elif "401" in e.args[0]:  # Tweets are protected
            return None
        else:
            raise e


def tweet_id_to_timestamp(tweet_id: int) -> datetime:
    """Convert tweet id to timestamp."""
    offset = 1288834974657
    tstamp = (tweet_id >> 22) + offset
    return datetime.utcfromtimestamp(tstamp / 1000)


def timestamp_to_tweet_id(timestamp: datetime) -> int:
    """Convert timestamp to twitter id."""
    offset = 1288834974657
    return ((int(timestamp.strftime("%s")) * 1_000) - offset) << 22
