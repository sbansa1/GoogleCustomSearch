#!/usr/local/bin/python3


import csv
import datetime
import sys
import time
import traceback
from multiprocessing.pool import ThreadPool

import requests
import json
from collections import defaultdict
import os
from requests.exceptions import ConnectionError,InvalidSchema,HTTPError
from ratelimit import limits, sleep_and_retry,RateLimitException

FIFTEEN_SECONDS = 900
start = time.time()
counter = 0
filename = "List-1.csv"
THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
my_file = os.path.join(THIS_FOLDER, 'List-1.csv')
destinationPath = os.path.dirname(os.path.abspath(__file__))
#path = os.path.join(destinationPath + "/")


def url():
    """

    :return: Scrapes the File in the desired output
    """

    path = (os.path.abspath( "" ))
    file_name = my_file
    print(file_name)
    url_path = []
    data = " "

    with open( file_name, "r" ) as readfile:
        reader = csv.reader( readfile )
        next( reader )
        for lines in reader:
            lines = (str( lines ).replace( "[", "" ).replace( "]", "" ))
            url_path.append( lines )

    return [url_path]

@sleep_and_retry
@limits(calls=10, period=FIFTEEN_SECONDS)
def script(url_path):
    count = 0
    csvData = defaultdict( list )

    for i in range( len( url_path ) ):

        try:
            # making the program to wait before making a request as we can only make 10 requests per secpnd
            data = requests.get( url_path[i].replace( "'", "" ) )

            if data.status_code == 200:
                response_data = json.loads(data.text)
                if response_data.get('items') is None:
                    continue

                if len(response_data) > 0:
                    for values in response_data['items']:
                        csvData["title"].append( values['title'] )
                        csvData['link'].append( values['link'] )
                        csvData['snippet'].append( values['snippet'] )

                    with open("output{}.csv".format(datetime.datetime.utcnow()), "w") as outfile:
                        writer = csv.writer( outfile )
                        writer.writerow( csvData.keys() )
                        writer.writerows(zip( *csvData.values() ) )

            else:
                if 200 <= data.status_code <= 299:
                    print( f'status code: {data.status_code} inside of 200 range' )
                else:
                    print( f'status code: {data.status_code} outside of 200 range' )
        except ConnectionError as error:
            print( "Bad Url " + error )
            continue
        except InvalidSchema as e:
            print( e )
            continue
        except HTTPError as e:
            print( e )
            continue
        except RateLimitException:
            traceback.print_exc( file=sys.stdout )
            sys.exit( 0 )
        finally:
            count = count + 1



def main():

    pool = ThreadPool(8)

    results = pool.map(script,url())

    pool.close()
    pool.join()

    return results


main()


print ("Elapsed Time: %s" % (time.time() - start))




