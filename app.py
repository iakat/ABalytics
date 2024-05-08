# abalytics
# given a http code, and a url,
# we return a list of slugs to increment in redis,
# so that we can do some analysis on error rates, etc.
# e.g
import asyncio
import json
import urllib
from collections import defaultdict
from itertools import product
from time import time

import websockets


class URLSlugify:
    def __init__(self, message):
        # to parse the url we use a built in python library: urllib
        self.message = message
        self.ident = message["job_data"]["ident"]
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
        prefixes = [
            (self.ident, "wget", self.wget_code),
            (self.ident, "response", self.response_code),
            (self.ident, "response", self.response_fuz),
        ]
        suffixes = [
            (self.domain, "fullpath", self.fullpath),
            (self.domain, "folder", self.path_first_folder),
            (self.domain, "file", self.path_last_folder),
        ]

        if self.is_shopify:
            suffixes.append((self.domain, "shopify"))

        if self.is_error:
            prefixes.append((self.ident, "error"))
        else:
            prefixes.append((self.ident, "success"))

        self.identifiers.update([_id[1] for _id in prefixes + suffixes])
        return [prefix + suffix for prefix, suffix in list(product(prefixes, suffixes))]


async def main():
    BIG_DICTIONARY = defaultdict(int)
    IDENTIFIERS = set()
    TIME = time()
    # connect to ws://archivebot.archivingyoursh.it:4568/stream
    uri = "ws://archivebot.archivingyoursh.it:4568/stream"
    async with websockets.connect(uri) as websocket:
        async for message in websocket:
            try:
                message = json.loads(message)
            except:
                print("error parsing message...")
                continue
            try:
                url_slugify = URLSlugify(message)
                for slugs in url_slugify.slugs:
                    BIG_DICTIONARY[":".join(slugs)] += 1
                IDENTIFIERS.update(url_slugify.identifiers)
            except Exception as e:
                import traceback

                # if it's a KeyError, just ignore it
                if isinstance(e, KeyError):
                    continue
                print("error parsing", message)
                traceback.print_exc()
            if time() - TIME > 1:
                TIME = time()
                for identifier in IDENTIFIERS:
                    print(f"TOP 3 SLUGS FOR {identifier}")
                    # to find it, we have to split :, and match on 1 element
                    candidates = [
                        (k, v)
                        for k, v in BIG_DICTIONARY.items()
                        if k.split(":")[1] == identifier
                        or (len(k) >= 5 and k.split(":")[4] == identifier)
                    ]
                    candidates.sort(key=lambda x: x[1], reverse=True)
                    for k, v in candidates[:3]:
                        print(k, v)
                print("END OF TOP 3 SLUGS")

if __name__ == "__main__":
    asyncio.run(main())
