#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  8 14:38:34 2019

@author: ejreidelbach

:DESCRIPTION: Scrapes combine data from 2000 to 2019 from ProFootball Reference
    'https://www.pro-football-reference.com/draft/2019-combine.htm'

:REQUIRES:
   
:TODO:
"""
 
#==============================================================================
# Package Import
#==============================================================================
import datetime
import glob
import numpy as np
import os  
import pandas as pd
import pathlib
import requests
import tqdm
import time

from bs4 import BeautifulSoup
from urllib3.util import Retry

#==============================================================================
# Reference Variable Declaration
#==============================================================================

#==============================================================================
# Function Definitions
#==============================================================================
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

def scrape_combine_information():
    '''
    Purpose: Scrape combine information for all players for all years
        that is provided by pro-football-reference.com (sports-reference)
        [https://www.pro-football-reference.com/draft/2019-combine.htm]
        
    Inputs
    ------
        NONE
    
    Outputs
    -------
        df_master : Pandas DataFrame
            contains combine information for all players from all scraped years
    '''  
    print('!------ Scraping Combine Data --------!')

    # process all available years
    year_end = datetime.datetime.now().year + 1
    year_start = 2000
    
    # Scrape data for all available years
    for year in tqdm.tqdm(list(range(year_start, year_end))):
        
        # scrape page for combine year
        url = ('https://www.pro-football-reference.com/draft/' 
               + str(year) + '-combine.htm')
        soup = soupify_url(url)
        
        # Test for a year with no data (happens in new year w/o combine)
        if 'Page Not Found' in str(soup):
            continue
        
        # Retrieve the HTML of the combine table
        table = soup.find('table', {'id':'combine'})
        
        # Convert the table to a dataframe
        df_year = pd.read_html(str(table))[0]
        df_year['combine_year'] = year
        
        # Drop Rows that contain header information (i.e. 'Player' == 'Player')
        df_year = df_year[df_year['Player'] != 'Player']
        
        # Create a `Drafted` variable which indicates if the player was drafted
        df_year['drafted'] = df_year['Drafted (tm/rnd/yr)'].apply(lambda x:
            False if pd.isna(x) else True)
        
        # Drop columns that are not needed
        df_year = df_year[['combine_year', 'Ht', 'Wt','40yd', 'Vertical', 'Bench', 
                           'Broad Jump', '3Cone', 'Shuttle', 'drafted']]
        
        # Rename remaining columns
        df_year = df_year.rename(columns = {'Ht' : 'combine_height',
                                            'Wt' : 'combine_weight',
                                            '40yd' : 'combine_40yd',
                                            'Vertical' : 'combine_vertical',
                                            'Bench' : 'combine_bench',
                                            'Broad Jump' : 'combine_broad',
                                            '3Cone' : 'combine_3cone',
                                            'Shuttle' : 'combine_shuttle'})
        
        # Retrieve player's nfl/ncaa ID and profile URL
        list_player_ids_nfl = []
        list_player_ids_ncaa = []
        for row in table.find_all('tr'):
            # the first column of each row contains the nfl data
            col = row.find('th')
            if col.text != 'Player':
                if col.find('a') is not None:
                    list_player_ids_nfl.append(col['data-append-csv'])
                else:
                    list_player_ids_nfl.append(np.nan)
            # after that, the 3rd column over has the ncaa data
            col = row.find_all('td')
            if col != []:
                if col[2].find('a') is not None:
                    list_player_ids_ncaa.append(
                            col[2].a['href'].split('/')[-1].split('.html')[0])
                else:
                    list_player_ids_ncaa.append(np.nan)
                    
        # Create the variables: 'id__ncaa' and 'id_nfl'
        df_year['id_ncaa'] = list_player_ids_ncaa
        df_year['id_nfl'] = list_player_ids_nfl
        
        # Drop rows with no values for `id_ncaa` AND `id_nfl`
        df_year = df_year.dropna(subset = ['id_ncaa', 'id_nfl'])
    
        # Write the file to csv
        df_year.to_csv('data_combine/combine_%s.csv' % (year), index = False)
        
        # Wait a second to slow down the rate of scraping
        time.sleep(1)
    
    merge_combine_years()

    return
        
def merge_combine_years():
    '''
    Purpose: Merge combine information for every year that has been scraped
        into a master file
        
    Inputs
    ------
        NONE
    
    Outputs
    -------
        df_master : Pandas DataFrame
            contains combine information for all players from all scraped years
    ''' 
    print('!------ Merging NFL Combine Years --------!')

    # ingest all available year file names
    files = glob.glob('data_combine/combine_2*.csv')
    
    # create a dataframe for storing all combine info
    df_master = pd.DataFrame()
    
    # iterate over each available file
    for file in files:
        # if this is the first file, set the file to be df_master
        if len(df_master) == 0:
            df_master = pd.read_csv(file)
        # otherwise, append the new info to the master dataframe
        else:
            df_master = df_master.append(pd.read_csv(file))
            
    # write file to disk
    df_master.to_csv('data_combine/combine_all_years.csv', index = False)
    
    return

def main():
    pass

#=============================================================================
# Working Code
#==============================================================================
if __name__ == "__main__":
    main()

## Set the project working directory
#path_dir = pathlib.Path('/home/ejreidelbach/Projects/draft-analytics-data')
#os.chdir(path_dir)

## Scrape Data for all years
#scrape_combine_information()    