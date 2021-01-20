from api_helpers import *
import time

# Get epoch times here: https://www.unixtimestamp.com/index.php

oct_15_2020 = 1602720000
nov_01_2020 = 1604188800
nov_02_2020 = 1604275200
nov_07_2020 = 1604707200
dec_01_2020 = 1606780800
jan_01_2021 = 1609459200
now = int(time.time())

extract_comments(file_name="conservative_comments_before", subreddit="conservative",
    start_utc=oct_15_2020, end_utc=nov_02_2020)

# extract_comments(file_name="csgo_comments", subreddit="globaloffensive",
#     start_utc=nov_01_2020, end_utc=now, new=True, cache_limit=200)