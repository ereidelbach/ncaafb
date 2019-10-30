#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 09:40:11 2019

@author: ejreidelbach

:DESCRIPTION:

:REQUIRES:
    Refer to the Package Import section of the script
    
:TODO:
    TBD
"""
 
#==============================================================================
# Package Import
#==============================================================================
import csv
import os  
import pandas as pd
import pathlib
import requests

from bs4 import BeautifulSoup
from code_python.sports_ref_utilities import rename_school, set_team_code

#==============================================================================
# Reference Variable Declaration
#==============================================================================

#==============================================================================
# Function Definitions
#==============================================================================
def scrape_team_rankings(min_year, max_year):
    '''
    Purpose: Scrape team rankings for all teams in the specified date range
        (i.e. from 'min_year' to 'max_year')
        
        Note:  if min_year == max_year, only one season will be scraped

    Inputs
    ------
        min_year : int
            Year in which to begin scraping
        max_year : int
            Year in which to end scraping 
    
    Outputs
    -------
        ncaa_team_rankings_2xxx.csv : csv file
            Contains team-based rankings for every team in the specified year 
            (e.g. AP rankings, strength of schedule, etc.). 
    '''    
    for year in range(min_year, max_year+1):
        # set the desired url for scraping purposes using the input date range   
        url = (f'https://www.sports-reference.com/cfb/years/{year}-ratings.html')
        
        # Rename various column headers
        if year <= 1995:
            headers = ['School', 'Conf', 'AP Rank', 'Won', 'Lost', 'Tied', 'srs_off',
                       'srs_def', 'srs', 'off_ppg', 'def_ppg', 'off_pass_ypa', 
                       'def_pass_ypa', 'off_rush_ypc', 'def_rush_ypc', 'off_ypp', 
                       'def_ypp']
        else:
            headers = ['School', 'Conf', 'AP Rank', 'Won', 'Lost', 'srs_off',
                       'srs_def', 'srs', 'off_ppg', 'def_ppg', 'off_pass_ypa', 
                       'def_pass_ypa', 'off_rush_ypc', 'def_rush_ypc', 'off_ypp', 
                       'def_ypp']
        
        # set the filename
        filename = f'Data/sports_ref/ncaa_rankings/ncaa_team_rankings_{year}.csv'
        
        # scrape data and write to disk
        print(f'###------- Scaping NCAA Team Rankings for {year} -------###')
        run_scraper(filename, headers, url, team = True)
        
        # read in scraped data
        df_year = pd.read_csv(filename)
    
        # insert team codes for each team and their respective opponent
        df_year['Team Code'] = set_team_code(df_year['School'].tolist(), year)
        
        # standardize team names for the player's team
        df_year = rename_school(df_year, 'School')
    
        # write to disk
        df_year.to_csv(filename, index = False)
    
    return

def run_scraper(filename, headers, url, team = False):
    '''
    Purpose: Scrape data from a specified page and write it to a CSV file

    Inputs
    ------
        filename : string
            Name of the file to be written to disk
        headers : list of strings
            List of column headers found in the data
        url : string
            URL of the site being scraped
    
    Outputs
    -------
        filename : csv file
            Contains team rankings to disk in a .csv file format
    '''        
    #Open the CSV writer
    with open(filename, 'w') as f:
        # Initialize the CSV file writer
        wr = csv.writer(f)
        
        # Write the headers of the CSV
        wr.writerow(headers)
    
        # Notify the user when it reaches the potential end of the data
        try:
            page_response = requests.get(url)
        except requests.exceptions.RequestException as e:
            print(e)
            
        # Parse the new request with BeautifulSoup
        page_content = BeautifulSoup(page_response.content, "html.parser")
        
        # Find the table of data on the requested page
        table = page_content.find("table", {'id' : 'ratings'})
    
        # If the page didn't load properly, stop the loop and try again
        try:
            output = [[td.text for td in row.find_all(
                    'td')] for row in table.select ('tr')]
        except:
            print('Error loading data from page')
            
        # obtain the player-ids for all players
        list_ids = []        
        for row in table.select('tr'):
            if row.find('td') == None:
                list_ids.append([])
            else:
                try: 
                    list_ids.append([row.find('td')['data-append-csv']])
                except:
                    list_ids.append([''])
            
            # merge with existing data
            merged_output = []
            for player, pid in zip(output, list_ids):
                merged_output.append(player + pid)
        else:
            merged_output = output
    
        # Remove any blank rows that exist for formatting/readability reasons
        writeout = [rowout for rowout in merged_output if rowout]   
        
        # Write out the rows of data
        wr.writerows(writeout)

    f.close()

    return

#==============================================================================
# Working Code
#==============================================================================

# Set the project working directory
path_dir = pathlib.Path('/home/ejreidelbach/Projects/CollegeFootball')
os.chdir(path_dir)

# iterate over every year from 2000 to 2019 and scrape date
scrape_team_rankings(1962, 2019)