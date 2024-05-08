# abalytics
# given a http code, and a url,
# we return a list of slugs to increment in redis,
# so that we can do some analysis on error rates, etc.
# e.g
import asyncio
import json
import os
import urllib
from collections import defaultdict
from itertools import product
from time import time

import websockets
from redis import asyncio as aioredis


class URLSlugify:
    def __init__(self, message):
        # to parse the url we use a built in python library: urllib
        self.message = message
        self.ident = message["job_data"]["ident"]
        self.ts = message["ts"]
        self.url = urllib.parse.urlparse(message["url"])
        self.response_code = str(message["response_code"])
        self.is_error = message["is_error"]
        self.wget_code = message["wget_code"]
        self.identifiers = set()

    @property
    def domain(self):
        """domain."""
        return self.url.netloc

    @property
    def fullpath(self):
        """fullpath."""
        return self.url.path

    @property
    def response_fuz(self):
        """eg 2xx, 3xx, 4xx, 5xx"""
        return f"{int(self.response_code) // 100}xx"

    @property
    def path_first_folder(self):
        return self.url.path.split("/")[1]

    @property
    def path_last_folder(self):
        return self.url.path.split("/")[-1]

    @property
    def is_shopify(self):
        return self.domain.endswith(".shopify.com")

    @property
    def slugs(self):
        # we return a list of slugs to increment in redis
        prefixes = (
            ("wget", (self.ident, self.wget_code)),
            ("response", (self.ident, self.response_code)),
            ("response", (self.ident, self.response_fuz)),
        )
        suffixes = (
            ("fullpath", (self.domain, self.fullpath)),
            ("folder", (self.domain, self.path_first_folder)),
            ("file", (self.domain, self.path_last_folder)),
        )

        if self.is_shopify:
            suffixes.append(("shopify", (self.ident)))

        if self.is_error:
            prefixes.append(("error", (self.ident)))
        else:
            prefixes.append(("success", (self.ident)))

        # now, flatten everything, so that we have like,
        # response:fullpath:ident:domain:path

        flattened = []
        for prefix, suffix in product(prefixes, suffixes):
            flattened.append((prefix[0], suffix[0], *prefix[1], *suffix[1]))
        return flattened
async def main():
    TIMEBUCKETS = [
        60,
        600,
        3_600,
        86_400,
    ]
    BIG_DICTIONARY = defaultdict(list)
    IDENTIFIERS = set()
    TIME = time()
    # connect to ws://archivebot.archivingyoursh.it:4568/stream
    uri = "ws://archivebot.archivingyoursh.it:4568/stream"
    R = await aioredis.create_redis_pool(os.getenv("REDIS_URL", "redis://localhost"))

    async with websockets.connect(uri) as websocket:
        async for message in websocket:
            try:
                message = json.loads(message)
            except:
                print("error parsing message...")
                continue
            try:
                to_add = defaultdict(list)
                url_slugify = URLSlugify(message)
                for slugs in url_slugify.slugs:
                    for bucket in TIMEBUCKETS:
                        to_add[f"{bucket}:{":".join(slugs)}"].append(url_slugify.ts)
                IDENTIFIERS.update(url_slugify.identifiers)
                with await R.pipeline() as pipe:
                    for key, value in to_add.items():
                        pipe.zadd(key, *value)
                    await pipe.execute()
            except Exception as e:
                import traceback

                # if it's a KeyError, just ignore it
                if isinstance(e, KeyError):
                    continue
                print("error parsing", message)
                traceback.print_exc()


# ^ above is the main loop,
# only one should be running.
# below is the analysis loop,
# any can run, as they just read the data from the redis and make analysis.
def analysis(self):

    async def main():
        R = await aioredis.create_redis_pool(
            os.getenv("REDIS_URL", "redis://localhost")
        )
        TIMEBUCKETS = [
            60,
            600,
            3_600,
            86_400,
        ]

        # let's start by ensuring the timebuckets are properly cleaned up with the appropriate timebuckets.

        async def cleanup():
            # target_time should be 5 seconds ago, just to make sure we don't deal with unfinished data.
            target_time = int(time()) - 5
            for bucket in TIMEBUCKETS:
                for key in await R.keys(f"{bucket}:*"):
                    await R.zremrangebyscore(key, 0, target_time)

        await cleanup()

        # then, we can start the analysis.

        for ident in []:
            for bucket in TIMEBUCKETS:
                for key in await R.keys(f"{bucket}:{ident}:*"):
                    print(key, await R.zrange(key, 0, -1))

    asyncio.run(main())


if __name__ == "__main__":
    asyncio.run(main())
