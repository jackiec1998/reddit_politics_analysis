# Credit: https://www.osrsbox.com/blog/2019/03/18/watercooler-scraping-an-entire-subreddit-2007scape/

import requests
import json
import re
import time
import datetime as dt
import pandas as pd
import os
import time
import sys

PUSHSHIFT_REDDIT_URL = "http://api.pushshift.io/reddit"

'''
    Optional Parameters:
        All (string): All the arguments given will be appended onto the request.
'''
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
        return response['data']
        # sorted_data_by_id = sorted(data, key=lambda x: int(x['id'], 36))
        # return sorted_data_by_id

    else:
        print("Request was unsuccessful. Error code below.")
        print(request.status_code)
        return None

'''
    Necessary Parameters:
        'subreddit' (string): The subreddit that is going to be queried.
        'start_utc' (int): Start of the study period in UTC. Although it will
            pick up where it left off if file already exists from most recent
            UTC timestamp.
        'end_utc' (int): End of the study period in UTC.

    Optional Parameters:
        'file_name' (string): Name of the CSV file (don't include .csv).
        'max_retries' (int): The maximum number of retries on a failed request
            (default is 5 tries).
        'sleep_duration' (int): The seconds sleeping after an API request
            (defaut is 0.5 seconds).
'''
def extract_comments(**kwargs):

    '''
        START: Intaking method and query arguments.
    '''

    if "subreddit" in kwargs.keys():
        subreddit = kwargs['subreddit']
    else:
        sys.exit("The subreddit is a necessary parameter.")

    # Get the wanted data points from desired_columns.txt file.
    with open("desired_columns.txt") as file:
        desired_columns = file.read().splitlines()

    # Define the file name.
    if "file_name" in kwargs.keys():
        CSV_file_name = kwargs['file_name'] + ".csv"
    else:
        CSV_file_name = "default.csv"

    # See if 'new' flag is True to delete the existing referred CSV file.
    if "new" in kwargs.keys() and os.path.exists(CSV_file_name):
        os.remove(CSV_file_name)

    # Define study period.
    if "start_utc" in kwargs.keys() and "end_utc" in kwargs.keys():
        start_utc = kwargs['start_utc']
        end_utc = kwargs['end_utc']

    # Define maximum retry attempts.
    if "max_retries" in kwargs.keys():
        max_retries = int(kwargs['max_retries'])
    else:
        max_retries = 5

    # Define sleep duration after each request.
    if "sleep_duration" in kwargs.keys():
        sleep_duration = int(kwargs['sleep_duration'])
    else:
        sleep_duration = 0.5

    '''
        END: Intaking method and query arguments.
    '''

    '''
        BEGIN: Pushshift API requests.
    '''

    time_requests_begin = time.time()

    df = None

    while True:

        # Read the database using the right index_col.
        if os.path.exists(CSV_file_name) and df is None:
            df = pd.read_csv(CSV_file_name, index_col=0)
        elif df is None:
            df = pd.DataFrame()

        objects = None
        retries = 0

        # Used to calculate the change in size with the new request.
        num_rows = df.shape[0]

        # Start off where you left off on the CSV.
        try:
            start_utc = int(df['created_utc'].max())
        except:
            print("Couldn't find a midway point to start from.")
            pass

        # Send request time.
        request_time = time.time()

        # Make a request until successful or reaches max retries.
        while (objects is None or len(objects) == 0) and retries < max_retries:
            print(f"Retries at: {retries}.")
            objects = send_request(subreddit=subreddit, after=start_utc, before=end_utc)
            retries += 1

        if objects is None or len(objects) == 0:
            print("The request returned nothing, stopping extraction. " \
                + "Check request error. Writing to CSV and exiting.")

            df = df.drop_duplicates()
            df.to_csv(CSV_file_name)
            
            sys.exit()

        print(f"Request return {len(objects)} and took {time.time() - request_time:.2f} seconds.")
            
        # Timing formating of objects.
        formatting_time = time.time()

        # Loop through the returned data and append to dataframe.
        for object in objects:

            # The object falls out of study period.
            if object['created_utc'] > end_utc:
                continue

            # Only keep the columns you want.
            object = {key: object[key] for key in desired_columns}

            # print(json.dumps(object, sort_keys=True, indent=2))
            df = df.append(object, ignore_index=True)


        # Removed duplicates.
        df = df.drop_duplicates()

        df.to_csv(CSV_file_name)


        # Display the number of new items, time formating, and new line.
        print((f"{df.shape[0]} - {num_rows} = {df.shape[0] - num_rows} "
            "new objects have been added to the CSV. "
            f"It took {time.time() - formatting_time:.2f}."))

        print(f"Sleeping for {sleep_duration} seconds.\n")
        time.sleep(sleep_duration)
    
    '''
        END: Pushshift API requests.
    '''

