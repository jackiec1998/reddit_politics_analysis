from api_helpers import *


nov_1_2020 = 1604188800
nov_7_2020 = 1604707200

new_start = 1604266570

extract_comments(file_name="conservative_comments", subreddit="conservative",
    start_utc=nov_1_2020, end_utc=nov_7_2020)

# Example.
# extract_comments(file_name="globaloffensive_comments", subreddit="globaloffensive",
#     start_utc=1606780800, end_utc=1609459200)