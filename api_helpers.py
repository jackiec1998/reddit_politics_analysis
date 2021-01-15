# Credit: https://www.osrsbox.com/blog/2019/03/18/watercooler-scraping-an-entire-subreddit-2007scape/

import requests
import json
import re
import time
import datetime as dt
import pandas as pd
import os.path as path
import time

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
        data = response['data']
        sorted_data_by_id = sorted(data, key=lambda x: int(x['id'], 36))
        return sorted_data_by_id

    else:
        print("Request was unsuccessful. Error code below.")
        print(request.status_code)
        return None

'''
    Necessary Parameters: 
        'start_utc' (int): Start of the study period in UTC. Although it will
            pick up where it left off if file already exists from most recent
            UTC timestamp.
        'end_utc' (int): End of the study period in UTC.

    Optional Parameters:
        'file_name' (string): Name of the CSV file (don't include .csv).
        'max_retries' (int): The maximum number of retries on a failed request
            (default is 5).
        Other (string): Other parameters, like 'subreddit' will be added into
            the request.
'''
def extract_comments(**kwargs):

    # Get the wanted data points from desired_columns.txt file.
    with open("desired_columns.txt") as file:
        desired_columns = file.read().splitlines()

    # Define the file name.
    if "file_name" in kwargs.keys():
        CSV_file_name = kwargs['file_name'] + ".csv"
        del kwargs['file_name']
    
    else:
        CSV_file_name = "default.csv"

    # Define study period.
    if "start_utc" in kwargs.keys() and "end_utc" in kwargs.keys():

        start_utc = kwargs['start_utc']
        end_utc = kwargs['end_utc']

        # start_utc = 1604361600 # Nov. 3rd
        # end_utc = 1604966400 # Nov. 10th

    # Maximum retry attempts.
    if "max_retries" in kwargs.keys():
        max_retries = kwargs['max_retries']
        del kwargs['max_retries']

    else:
        max_retries = 5

    while True:

        # Read the database using the right index_col.
        if path.exists(CSV_file_name):
            df = pd.read_csv(CSV_file_name, index_col=0)
        else:
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
        time_sent_request = time.time()

        # Make a request until successful or reaches max retries.
        while objects is None and retries < max_retries:
            objects = send_request(subreddit=kwargs['subreddit'], before=end_utc, after=start_utc)
            retries += 1

        if objects is None:
            print("The request returned nothing, stopping extraction. " \
                + "Check request error. Writing to CSV and exiting.")

            df = df.drop_duplicates()
            df.to_csv(CSV_file_name)
            
            return

        # How long it took to get the request.
        request_duration = time.time() - time_sent_request

        print("Request returned {0} objects. It took {1} seconds.".format(len(objects), request_duration))
            
        # Timing formating of objects.
        time_formatting = time.time()

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

        formatting_duration = time.time() - time_formatting

        # Display the number of new items, time formating, and new line.
        print("{0} - {1} = {2} new objects have been added to the CSV.".format(df.shape[0], num_rows, df.shape[0] - num_rows))
        print("Formating the data took {0} seconds.".format(formatting_duration))
        print("\n")
