#-------------------------------------------------------------------------------
# Name:        Python API Request Example
#
# Author:     Joel Adebayo
#              
# Created:     4/12/2020
# Copyright:   (c) Adebayo, Joel
#-------------------------------------------------------------------------------

import concurrent.futures
from functools import \
    lru_cache  # Use least recently used cache for faster performance

import pandas as pd
import requests  # Used to send http requests

#set up a URL query for the request.
params = {
    'key': 'UNTDKBMLdLaqZJogsq53pMx3M1AM5Z43',
}

#Location to the csv file containing the coordinates
csvInput = r'PATH TO CSV FILE'
csvOutput = r'PATH TO OUTPUT CSV FILE'

@lru_cache(maxsize=5)
def sendReq (urlInfo): 
    try:
        r = requests.get(urlInfo,params)
        geocodeJson = r.json()
        return geocodeJson['addresses'][0]['address']
    except Exception as e:
        return e

#open csvInput (input) for reading and open csvOutput (output) for writing.
read_df = pd.read_table(csvInput,delimiter =',')
urlList = []
for x in range(len(read_df.index)):
    lat = read_df.loc[x,['Latitude']].values[0]
    _long = read_df.loc[x,['Longitude']].values[0]
    url = f'https://api.tomtom.com/search/2/reverseGeocode/{lat},{_long}.JSON'
    urlList.append(url)

#Use multithreading to excute the requests in parellel.
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = executor.map(sendReq,urlList)

#Create new Dataframe with results data
data = [{
        'streetNumber':val['streetNumber'],
        'streetName':val['streetName'],
        'municipality':val['municipality'],
        'postalCode': val['postalCode'],
        'country': val['country']
        } for val in results]

write_df = pd.DataFrame(data)
write_df.index.name ='uuid'

#Write results to new csv file
write_df.to_csv(csvOutput)
