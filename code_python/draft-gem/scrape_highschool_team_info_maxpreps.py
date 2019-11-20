#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 12:08:11 2019

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
import ast
import os  
import pandas as pd
import pathlib
import requests
import tqdm

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

def scrape_team_info():
    '''
    Purpose: Given high school elo data, extract the opponent IDs and
        scrape information for each school if it does not already exist

    Inputs   
    ------
        NONE
            
    Outputs
    -------
        
    '''
    # read in list of teams
    list_teams = ingest_list_teams()    
    
    # scrape info on each team
    list_team_info = []
    for team in tqdm.tqdm(list_teams):
        # create URL for team on maxpreps.com
        url = f'https://www.maxpreps.com/high-schools/{team}/football/home.htm'
        
        # scrape team's site
        soup = soupify_url(url)
        
        # create dictionary for storing team information
        dict_team = {}
        dict_team['TeamCode'] = team
        dict_team['URL_max'] = url
        
        # EXTRACT: NICKNAME
        try:
            dict_team['Nickname'] = soup.find('dd',
                     {'id':('ctl00_NavigationWithContentOverRelated_Content' + 
                      'OverRelated_PageHeaderUserControl_MascotName')}).text
        except:
            dict_team['Nickname'] = ''
            
        # EXTRACT: CITY, STATE
        try:
            address = soup.find('dd', {
                    'id':('ctl00_NavigationWithContentOverRelated_' +
                    'ContentOverRelated_PageHeaderUserControl_Location')}).text
            dict_team['Name1'] = address.split(',')[1].strip() + ', ' + (
                    address.split(', ')[2].split(' ')[0].strip())
        except:
            dict_team['Name1'] = ''
        
        # EXTRACT: TEAM LOGO
        try:
            dict_team['URL'] = soup.find('img', 
                     {'id':('ctl00_NavigationWithContentOverRelated_' + 
                      'ContentOverRelated_PageHeaderUserControl_Image')})['src']
        except:
            dict_team['URL'] = ''

        # add team info to list
        list_team_info.append(dict_team)
   
    # convert list of teams into a dataframe
    df_teams = pd.DataFrame(list_team_info)
    
    # create the 'Team' variable
    list_schools = []
    for index, row  in df_teams.iterrows():
        # extract TeamCode and City,State for the school
        code = row['TeamCode']
        city_state = row['Name1']
        
        # extract name of school
        school = ' '.join(code.split('-(')[0].split('-')[:-1]).title()
        
        # lower case specific words
        school = school.replace('Of','of')
        
        # add on state of school
        school = school + ' (' + city_state.split(', ')[1].strip() + ')'
        
        list_schools.append(school)
    df_teams['Team'] = list_schools
    
    # write to disk
    df_teams.to_csv('/home/ejreidelbach/Downloads/highschool_teams.csv', 
                    index = False)
                    
    return
    
def ingest_list_teams():
    '''
    Purpose: Given high school elo data, extract the IDs for all opponents

    Inputs   
    ------
        NONE
            
    Outputs
    -------
        list_teams : list of strings
            List of all opponent IDs contained in the specified Elo data
    '''    
    # set filepath to elo data
    filename = '/home/ejreidelbach/Downloads/highschool_elo.csv'
    
    # read in the elo data
    df_elo = pd.read_csv(filename)
    
    # create a list of all opponents
    list_teams = []
    for list_opp in df_elo['QB_opp']:
        for opp in ast.literal_eval(list_opp):
            if opp != 0:
                list_teams.append(opp)
        
    # dedup list and sort order
    list_teams = sorted(set(list_teams))

    return list_teams
    
#==============================================================================
# Working Code
#==============================================================================

# Set the project working directory
path_dir = pathlib.Path('/home/ejreidelbach/Projects/draft-analytics-api/src/data')
os.chdir(path_dir)

scrape_team_info()