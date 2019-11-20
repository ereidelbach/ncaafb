#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Fri Oct  4 11:23:40 2019

@author: ejreidelbach

:DESCRIPTION:
    Obtain recruiting data from api.collegefootballdata.com and infuse it
    with geo dat from https://nces.ed.gov/programs/edge/Geographic/SchoolLocations

:REQUIRES:
    Refer to the Package Import section of the script
    
:TODO:
    TBD
"""
 
#==============================================================================
# Package Import
#==============================================================================
from code_python.config import bing_key
import geopy
import json
import os  
import pandas as pd
import pathlib
import requests
import time
import tqdm
import wikipedia

from bs4 import BeautifulSoup
from urllib3.util import Retry
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys

#==============================================================================
# Reference Variable Declaration
#==============================================================================

#==============================================================================
# Function Definitions
#==============================================================================
def rename_school(schools):
    '''
    Purpose: Rename a school/university to a standard name as specified in 
        the file `teams_ncaa.csv`

    Inputs
    ------
        schools : Pandas Series
            All school names that require standardization
    
    Outputs
    -------
        fixed_schools : Pandas Series
            DataFrame containing the standardized team name 
    '''  
    # read in school name information
    df_school_names = pd.read_csv('data_team/teams_ncaa.csv')
     
    # convert the dataframe to a dictionary such that the keys are the
    #   optional spelling of each school and the value is the standardized
    #   name of the school
    dict_school_names = {}
    
    for index, row in df_school_names.iterrows():
        # isolate the alternative name columns
        names = row[[x for x in row.index if 'Name' in x]]
        # convert the row to a list that doesn't include NaN values
        list_names = [x.strip() for x in names.values.tolist() if str(x) != 'nan']
        # add the nickname to the team names as an alternative name
        nickname = row['Nickname'].strip()
        list_names_nicknames = list_names.copy()
        for name in list_names:
            list_names_nicknames.append(name + ' ' + nickname)
        # extract the standardized team name
        name_standardized = row['Team'].strip()
        # add the standardized name
        list_names_nicknames.append(name_standardized)
        # add the nickname to the standardized name
        list_names_nicknames.append(name_standardized + ' ' + nickname)
        # for every alternative spelling of the team, set the value to be
        #   the standardized name
        for name_alternate in list_names_nicknames:
            dict_school_names[name_alternate] = name_standardized
            
    def swapSchoolName(name_old):
        if ((name_old == 'nan') or (pd.isna(name_old)) or 
             (name_old == 'none') or (name_old == '')):
            return ''
        try:
            return dict_school_names[name_old]
        except:
            print('Did not find: %s' % (name_old))
            return name_old
            
    fixed_schools = schools.apply(lambda x: swapSchoolName(
            x.strip().replace('é','e')))
    
    return fixed_schools

def soupify_url(url):
    '''
    Purpose: Turns a specified URL into BeautifulSoup formatted HTML 

    Inputs
    ------
        url : string
            Link to the designated website to be scraped
    
    Outputs
    -------
        soup : html
            BeautifulSoup formatted HTML data stored as a complex tree of 
            Python objects
    '''
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    r = session.get(url)
    #r = requests.get(url)
    soup = BeautifulSoup(r.content,'html.parser')   
    
    return soup

def query_api(year, classification):
    '''
    Purpose: Retrieve recruiting data for the desired year from the 
        collegefootballdata.com API

    Inputs   
    ------
        year : int
            Desired year for which to scrape recruiting data
        classification : string
            type of recruit (HighSchool, JUCO, PrepSchool)
            
    Outputs
    -------
        list_players : list of dictionaries
            list of recruits stored in dictionary format
    '''
    # query the API
    r = requests.get(
            'https://api.collegefootballdata.com/recruiting/players?' + 
            f'year={year}&classification={classification}')
    
    # convert json response (i.e. string) to list of dictionaries
    list_players = json.loads(r.text)
    
    return list_players

def geolocate_schools(df_players):
    '''
    Purpose: Retrieve recruiting data for the desired year from the 
        collegefootballdata.com API

    Inputs   
    ------
        df_players : Pandas DataFrame
            Contains recruiting data on all players for a given year
            
    Outputs
    -------
        df_players_geo : Pandas DataFrame
            Updated dataframe with lat/long locations for each high school
    '''
#    geolocator = geopy.geocoders.Bing(api_key = bing_key)

    # obtain all unique high schools to search
    df_players = pd.read_csv('data_recruiting/recruiting_all_years.csv')
    df_schools = df_players[df_players['recruitType'] == 'HighSchool']
    df_schools = df_schools[df_schools['country'] == 'United States']
    df_schools = df_schools[['school', 'state']].drop_duplicates()
    
    # obtain all unique post-secondary schools (i.e. JUCOs and Prep Schools)
    df_secondary = df_players[df_players['recruitType'] != 'HighSchool']
    df_secondary = df_secondary[df_secondary['country'] == 'United States']
    df_secondary = df_secondary[['school']].drop_duplicates()
    
    # initialize browser for scraping    
    options = Options()
    options.headless = True
    browser = webdriver.Firefox(options=options)           

    list_addresses = []
    for index, row in tqdm.tqdm(df_players.iterrows()):
        address = (row['school'] + ' High School ' + row['state'])        
        # retrieve beautiful-soup version of the data        
        browser.get('http://www.google.com')
        browser.implicitly_wait(3)
        search = browser.find_element_by_name('q')
        search.send_keys(address)
        search.send_keys(Keys.RETURN) # hit return after you enter search text
        time.sleep(2)
        
        try:
            html = browser.page_source
            soup = BeautifulSoup(html, 'html.parser')
        except:
            list_addresses.append('')
            print(f'Error at {index}: {address}')
            continue
            
        try:
            list_addresses.append(soup.find('div', {
                    'data-attrid':'kc:/location/location:address'}).text)  
        except:
            list_addresses.append('')
            print(f'Error at {index}: {address}')

    browser.quit()      
     
#    #--------------- WIKIPEDIA -----------------------------------------------#
#    list_addresses = []
#    for index, row in tqdm.tqdm(df_players.iterrows()):
#        address = (row['school'] + ' High School, ' + row['state'])
#        
#        soup = BeautifulSoup(wikipedia.page(address).html(), 'html.parser')
#        try:
#            table = soup.find('tr', {'class':'adr'}).find_all('div')
#            list_addresses.append(', '.join(x.text for x in table))
#        except:
#            list_addresses.append('')
#            print(f'Error at {index}: {address}')    
    
    
    
#    # retrieve latitude
#    list_lat.append(wikipedia.page(address).coordinates[0])
#    # retrieve longitude
#    list_lon.append(wikipedia.page(address).coordinates[1])
    
#    # read in private school information
#    df_schools = pd.read_excel(
#            'data_recruiting/schools_private_1718.xlsx',
#            usecols = ['NAME', 'CITY', 'STATE', 'LAT', 'LON'])
#    # read in public school information
#    df_schools = df_schools.append(
#            pd.read_excel('data_recruiting/schools_public_1718.xlsx',
#                          usecols = ['NAME', 'CITY', 'STATE', 'LAT', 'LON']))
#    # remove kindergartens
#    df_schools = df_schools[~df_schools['NAME'].str.contains('Kindergarten')]
#    df_schools = df_schools[~df_schools['NAME'].str.contains('Kindercare')]
#    df_schools = df_schools[~df_schools['NAME'].str.contains('Montessori')]
#    # remove elementary schools
#    df_schools = df_schools[~df_schools['NAME'].str.contains('Elementary')]
#    # remove middle schools
#    df_schools = df_schools[~df_schools['NAME'].str.contains('Middle School')]
#    
#    # rename columns as desired
#    df_schools = df_schools.rename(columns = {'NAME':'school',
#                                              'CITY':'city',
#                                              'STATE':'state',
#                                              'LAT':'school_lat',
#                                              'LON':'school_lon'})
#    
#    # create columns for merging that are lower case
#    df_schools['school_lower'] = df_schools['school'].str.lower()
#    df_schools['city_lower'] = df_schools['city'].str.lower()
#    df_schools['state_lower'] = df_schools['state'].str.lower()
#    df_players['school_lower'] = df_players['school'].str.lower()
#    df_players['city_lower'] = df_players['city'].str.lower()
#    df_players['state_lower'] = df_players['state'].str.lower()
#    
#    # attempt to match lat/long values on school name, city and state
#    df_merge = pd.merge(df_players, 
#                        df_schools,
#                        how = 'left',
#                        left_on = ['school_lower', 'city_lower', 'state_lower'],
#                        right_on = ['school_lower', 'city_lower', 'state_lower'])
#    
#    return df_merge
        
    browser.quit()
    
    return

def retrieve_recruiting_data(year):
    '''
    Purpose: Retrieve recruiting data for the desired year from the 
        collegefootballdata.com API

    Inputs   
    ------
        year : int
            Desired year for which to scrape recruiting data
            
    Outputs
    -------
        df_players : Pandas DataFrame
            Contains recruiting data on all players for a given year
    '''
    # create a dataframe for storing recruits
    df_players = pd.DataFrame()
    
    # iterate over all types of recruits
    for classification in ['HighSchool', 'JUCO', 'PrepSchool']:
        # query the API to retreive the type of recruits
        list_players = query_api(year, classification)
        
        # add the scraped players to the master dataframe
        if len(df_players) == 0:
            df_players = pd.DataFrame(list_players)
        else:
            df_players = df_players.append(pd.DataFrame(list_players))
    
    # reset index of dataframe
    df_players = df_players.reset_index(drop = True)

    # drop players with no 'committedTo' value
    df_players = df_players.dropna(subset = ['committedTo'])
    
    # fill in NONE values with empty strings for locations
    df_players['city'] = df_players['city'].apply(
            lambda x: '' if pd.isna(x) else x)
    df_players['stateProvince'] = df_players['stateProvince'].apply(
            lambda x: '' if pd.isna(x) else x)
    
    # fill in missing country values with 'United States'
    df_players['country'] = df_players['country'].apply(
            lambda x: 'United States' if pd.isna(x) else x)
       
    # standardize NCAA school names
    df_players['committedTo'] = rename_school(df_players['committedTo'])

    # rename columns as desired
    df_players = df_players.rename(columns = {'stateProvince':'state'})

    # write to disk
    df_players.to_csv(f'data_recruiting/recruiting_{year}.csv', index = False)
        
    return

#==============================================================================
# Working Code
#==============================================================================
#for year in tqdm.tqdm(range(2000, 2019)):
#    retrieve_recruiting_data(year)
#    
#df_all = pd.DataFrame()
#for year in tqdm.tqdm(range(2000, 2019)):
#    if len(df_all) == 0:
#        df_all = pd.read_csv(f'data_recruiting/recruiting_{year}.csv')
#    else:
#        df_all = df_all.append(pd.read_csv(f'data_recruiting/recruiting_{year}.csv'))
#        
#df_all = df_all.sort_values(by = ['year', 'name', 'recruitType'])
#df_all = df_all.reset_index(drop = True)
#df_all.to_csv('data_recruiting/recruiting_all_years.csv', index = False)