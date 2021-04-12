#-------------------------------------------------------------------------------
# Name:         Python API Request Example
#
# Author:       Joel Adebayo
#              
# Created:      4/12/2020
# Copyright:    (c) Adebayo, Joel
#
# Objective:    Reverse geocode coordinates from a CSV file and print the address
#-------------------------------------------------------------------------------

import concurrent.futures #For sending api requests concurrently

import pandas as pd #For reading a writing csv files; Install openpyxl dependency to use pandas
import requests  # Used to send http requests
import requests_cache #For caching api results
import sys

#Send a GET request to tomtom api
def get_address (url): 
    try:
        r = requests.get(url,params)
        geocodeJson = r.json()
        return geocodeJson['addresses'][0]['address']
        
    except Exception as e:
        return e

if __name__ == '__main__':

    #Specify parameters for api request
    params = {
        'key': 'shkEPTAKIwt6G92CdZAUEuw8U9pWbwfZ',
    }

    #Use computer memory for api caching
    requests_cache.install_cache(backend='memory')

    #Read input CSV file and create a list of urls
    read_df = pd.read_excel(sys.argv[1], engine='openpyxl')

    urlList = [(f'https://api.tomtom.com/search/2/reverseGeocode/{read_df.loc[x,["Latitude"]].values[0]},'
                f'{read_df.loc[x,["Longitude"]].values[0]}.JSON') for x in range(len(read_df.index))]


    #Use multithreading to excute the requests in parellel.
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        results = executor.map(get_address,urlList)

    data = [f'{val["streetNumber"]} {val["streetName"]}, {val["municipality"]} {val["postalCode"]}'\
            for val in results]
    

    #Print addresses
    for address in data:
        sys.stdout.write(address + '\n')
          
    #Remove cache
    requests_cache.uninstall_cache()
