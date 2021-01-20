from api_helpers import *

# Get epoch times here: https://www.unixtimestamp.com/index.php

nov_01_2020 = 1604188800
nov_07_2020 = 1604707200
dec_01_2020 = 1606780800
jan_01_2021 = 1609459200
now = int(time.time())

extract_comments(file_name="democrats_comments_november", subreddit="democrats",
    start_utc=nov_01_2020, end_utc=now)

# extract_comments(file_name="csgo_comments", subreddit="globaloffensive",
#     start_utc=nov_01_2020, end_utc=now, new=True, cache_limit=200)