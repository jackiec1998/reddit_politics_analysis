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
import atexit

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
    date = dt.datetime.fromtimestamp(int(kwargs['after'])).strftime('%Y-%m-%d, %H:%M:%S')
    print(f"{date} requested.")

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

def save(main_df, cache_df, file_name):

    if not cache_df.empty:
        main_df = main_df.append(cache_df, ignore_index=True)

        main_df = main_df.drop_duplicates()

        main_df.to_csv(file_name)

        return main_df
    else:
        print("Given cache dataframe is empty.")
        sys.exit()


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

    #region Intaking arguments.

    # Checking if subreddit is an argument given.
    if "subreddit" in kwargs.keys():
        subreddit = kwargs['subreddit']
    else:
        sys.exit("The subreddit is a necessary parameter.")

    # Get the wanted data points from desired_columns.txt file.
    with open("desired_columns.txt") as file:
        desired_columns = file.read().splitlines()

    # Define the file name.
    if "file_name" in kwargs.keys():
        file_name = kwargs['file_name'] + ".csv"
    else:
        file_name = "default.csv"

    # See if 'new' flag is True to delete the existing referred CSV file.
    if "new" in kwargs.keys() and kwargs["new"] is True and os.path.exists(file_name):
        os.remove(file_name)

    # Define study period.
    if "start_utc" in kwargs.keys() and "end_utc" in kwargs.keys():
        start_utc = kwargs['start_utc']
        end_utc = kwargs['end_utc']
    else:
        sys.exit("The study period needs to be defined using 'start_utc' and 'end_utc'.")

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

    if "cache_limit" in kwargs.keys():
        cache_limit = int(kwargs['cache_limit'])
    else:
        cache_limit = 5_000

    #endregion

    '''
        Arguments that should be defined at this point:
            - subreddit (string)
            - desired_columns (list of strings)
            - CSV_file_name (string)
            - start/end_utc (string)
            - max_retries (int)
            - sleep_duation (float/int)
            - cache_limit (int)
    '''

    #region Sending requests loop.

    # Finding the main CSV file for the dataframe.
    if os.path.exists(file_name):
        main_df = pd.read_csv(file_name, index_col=0)
    else:
        main_df = pd.DataFrame()

    # Defining the cache dataframe because writing constantly to the main dataframe is dumb.
    cache_df = pd.DataFrame()

    # Finding a midway point to start from if there exists one.
    if not main_df.empty:
        start_utc = int(main_df['created_utc'].max())

    while True:

        try:

            objects = None
            retries = 0

            # Start off where you left off either in the cache of main dataframe.
            if not cache_df.empty:
                start_utc = int(cache_df['created_utc'].max())
            elif not main_df.empty:
                start_utc = int(main_df['created_utc'].max())

            # Send request time.
            request_time = time.time()

            # Make a request until successful or reaches max retries.
            while (objects is None or len(objects) == 0) and retries <= max_retries:
                
                if retries > 0:
                    print(f"Retry {retries}!")
                
                objects = send_request(subreddit=subreddit, after=start_utc, before=end_utc)

                retries += 1

            if objects is None or len(objects) == 0:
                
                print("The request returned nothing, stopping extraction. " \
                    + "Check request error. Writing to CSV and exiting.")


                save(main_df, cache_df, file_name)
                
                sys.exit()

            print(f"{len(objects)} / {time.time() - request_time:.2f} objects returned/response time.")
                
            # Timing formating of objects.
            formatting_time = time.time()

            # Loop through the returned data and append to dataframe.
            for object in objects:

                # The object falls out of study period.
                if object['created_utc'] > end_utc:
                    print("Found an object outside of study period.")
                    continue

                # Only keep the columns you want.
                object = {key: object[key] for key in desired_columns}

                # print(json.dumps(object, sort_keys=True, indent=2))
                cache_df = cache_df.append(object, ignore_index=True)


            # Check to clear cache.
            if cache_df.shape[0] > cache_limit:
                print("--- Cache hit limited. Writing to CSV file and emptying cache. ---")

                main_df = save(main_df, cache_df, file_name)

                cache_df = pd.DataFrame()
            
            print(f"{cache_df.shape[0]} / {main_df.shape[0]} objects in cache/main.")


            # Display the number of new items, time formating, and new line.
            print((f"{time.time() - formatting_time:.2f} seconds for formatting."))

            print(f"{sleep_duration} seconds for sleeping.\n")
            time.sleep(sleep_duration)

        except KeyboardInterrupt:
            
            print("Keyboard interruption caused program to stop. Writing to CSV file.")
        
            to_csv_time = time.time()

            save(main_df, cache_df, file_name)

            print(f"It took {time.time() - to_csv_time} seconds to write to CSV.")

            sys.exit()

        except Exception as e:
            
            to_csv_time = time.time()

            save(main_df, cache_df, file_name)

            print(f"It took {time.time() - to_csv_time} seconds to write to CSV.")
            
            print("Unhandlded error message below.")

            print(e.message)

            sys.exit()


    
    #endregion