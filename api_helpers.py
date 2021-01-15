# Credit: https://www.osrsbox.com/blog/2019/03/18/watercooler-scraping-an-entire-subreddit-2007scape/

import requests
import json
import re
import time
import datetime as dt
import pandas as pd
import os.path as path

PUSHSHIFT_REDDIT_URL = "http://api.pushshift.io/reddit"

def send_request(**kwargs):

    # Default parameters for Pushshift API query.
    params = {
        "sort_type": "created_utc",
        "sort": "asc",
        "size": 100 # Because Pushshift limits requests to 100 entries.
    }

    # Add other request parameters.
    for key, value in kwargs.items():
        params[key] = value

    # Print out the parameters and readable time.
    print(params)
    print(dt.datetime.fromtimestamp(int(kwargs['after'])).strftime('%Y-%m-%d, %H:%M:%S'))

    type = "comment"

    # Execute the request.
    request = requests.get(PUSHSHIFT_REDDIT_URL + "/" + type + "/search/", params=params, timeout=30)

    if request.status_code == 200:
        response = json.loads(request.text)
        data = response['data']
        sorted_data_by_id = sorted(data, key=lambda x: int(x['id'], 36))
        return sorted_data_by_id

    else:
        print("Request was unsuccessful. Error code below.")
        print(request.status_code)
        return None

def extract_comments(**kwargs):

    with open("desired_columns.txt") as file:
        desired_columns = file.read().splitlines()

    CSV_file_name = "conservative_comments.csv"

    # Study period.
    start_utc = 1604361600 # Nov. 3rd
    end_utc = 1604966400 # Nov. 10th

    # Maximum retry attempts.
    max_retries = 5

    while True:

        # Read the database using the right index_col.
        if path.exists(CSV_file_name):
            df = pd.read_csv(CSV_file_name, index_col=0)
        else:
            df = pd.DataFrame()

        objects = None
        retries = 0

        num_rows = df.shape[0]

        # Start off where you left off on the CSV.
        start_utc = df['created_utc'].max()

        while objects is None and retries < max_retries:
            
            # Make a request.
            objects = send_request(**kwargs, after=start_utc)

        if objects is None:
            print("The request returned nothing, stopping extraction. Check request error.")

        print("Request returned {0} objects".format(len(objects)))
            
        # Loop the return data, ordered by date.
        for object in objects:

            # Grab the creation date.
            created_utc = object['created_utc']

            # Move forward the creation date based on the most recent object.
            if created_utc > start_utc:
                start_utc = created_utc
            
            # The object falls out of study period.
            if created_utc > end_utc:
                continue

            # Only keep the columns you want.
            object = {key: object[key] for key in desired_columns}

            # print(json.dumps(object, sort_keys=True, indent=2))
            df = df.append(object, ignore_index=True)


        # Removed duplicates.
        df = df.drop_duplicates()

        df.to_csv(CSV_file_name)

        # Display the number of new items.
        print("{0} - {1} = {2} new objects have been added to the CSV.".format(df.shape[0], num_rows, df.shape[0] - num_rows))

        print("\n")

        # time.sleep(0.5)