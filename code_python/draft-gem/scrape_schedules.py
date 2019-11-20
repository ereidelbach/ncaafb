#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 26 10:09:45 2019

@author: ejreidelbach

:DESCRIPTION: Scrapes information from missing player data (obtained from
            sports-reference.com).  Primary missing data obtained is `School`.

:REQUIRES:
   
:TODO:
"""
 
#==============================================================================
# Package Import
#==============================================================================
import json
import os  
import pandas as pd
import pathlib
import requests
import tqdm

from bs4 import BeautifulSoup
from requests.packages.urllib3.util.retry import Retry
from code_python.standardize_names_and_logos import rename_teams

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

def scrape_schedule_nfl(year = 2019):
    '''
    Purpose: Scrapes the schedule of games for an upcoming season from 
        pro-football-reference.com

    Inputs
    ------
        year : int
            The desired season for which to scrape games (Note: 2019 refers
            to the 2019-2020 season)
    
    Outputs
    -------
        df_schedule : Pandas DataFrame
            The full schedule of the desired season (for all teams)
    '''
    # scrape page and  
    url = ('https://www.pro-football-reference.com/years/' 
           + str(year) + '/games.htm')
    soup = soupify_url(url)
    
    # Ensure data has been found and no '404 error' exists:
    if 'Page Not Found (404 error)' in str(soup):
        pass
    else:
        # read the schedule html into a DataFrame
        df_schedule = pd.read_html(str(soup.find('div', {'id':'div_games'})))[0]
        
        # drop rows which are duplications of the column header
        df_schedule = df_schedule[df_schedule['Week'] != 'Week']
        # drop rows which contain values of NaN for the 'week' variable
        df_schedule = df_schedule[~pd.isna(df_schedule['Week'])]

        # remove preseason games
        df_schedule = df_schedule[~df_schedule['Week'].str.contains('Pre')]
        
        # handle new seasons (with no games played)
        if len(df_schedule.columns) < 10:
            print ('Formatting of season date has changed. Review changes ' + 
                   'and update the scraping code contained in this file.')
            return
        else:
            # insert season variable
            df_schedule['season'] = year
            
            # rename columns
            df_schedule = df_schedule.rename(columns = {'Week':'week',
                                                        'Date':'date',
                                                        'Unnamed: 5':'at'})
    
            # keep only desired columns
            df_schedule = df_schedule[['season', 'week', 'date', 'Winner/tie',
                                       'at', 'Loser/tie', 'PtsW', 'PtsL']]
   
            # cast variables as proper types
            df_schedule['PtsW'] = df_schedule['PtsW'].astype(float)
            df_schedule['PtsL'] = df_schedule['PtsL'].astype(float)
             
        # clean up dataframe
        df_schedule = restructure_nfl_schedule(df_schedule, year)
    
    # write to disk    
    df_schedule.to_csv('data_team/schedules/schedule_nfl_%i.csv' % year, index = False)
    
    return

def restructure_nfl_schedule(df, year):
    '''
    Purpose: Restructure a schedule from pro-football-reference.com into the 
        desired format.

    Inputs
    ------
        df : Pandas DataFrame
            Contains the schedule of all games from a single NCAA season
        year : int
            Desired year in which to restructure a desired schedule
    
    Outputs
    -------
        df : Pandas DataFrame
            Cleaned up version of original dataframe
    '''         
    # add the year to all dates as it does not exist
    list_dates = []
    for row in df['date']:
        # Account for the change of year
        if (('January' in row) or ('February' in row)):
            list_dates.append(row + ', ' + str(year + 1))
        else:
            list_dates.append(row + ', ' + str(year))
    df['date'] = list_dates
    
    # convert dates from strings to datetimes format
    df['date'] = pd.to_datetime(df['date'])
    
    # convert dates to 'YYYYMMDD' format
    df['date'] = df['date'].dt.strftime(
            '%Y%m%d').astype(int)
    
    # create home and away teams and home/away points
    list_home = []
    list_home_pts = []        
    list_away = []
    list_away_pts = []
    
    for index, row in df.iterrows():
        if pd.isna(row['at']):
            list_home.append(row['Winner/tie'])
            list_home_pts.append(row['PtsW'])
            list_away.append(row['Loser/tie'])
            list_away_pts.append(row['PtsL'])
        else:
            list_home.append(row['Loser/tie'])
            list_home_pts.append(row['PtsL'])
            list_away.append(row['Winner/tie'])
            list_away_pts.append(row['PtsW'])
    df['home_team'] = list_home
    df['home_points'] = list_home_pts
    df['away_team'] = list_away
    df['away_points'] = list_away_pts
                
    # standardize team names            
    df['home_team'] = rename_teams(df['home_team'], 'nfl', 'Team')
    df['away_team'] = rename_teams(df['away_team'], 'nfl', 'Team')
    
    # create team IDs for each team
    df['home_code'] = rename_teams(df['home_team'], 'nfl', 'TeamCode')
    df['away_code'] = rename_teams(df['away_team'], 'nfl', 'TeamCode') 
    
    # remove unnecessary columns and reorder variables
    df = df[['season', 'week', 'date', 'home_team', 'home_points', 'home_code',
             'away_team', 'away_points', 'away_code']]

    return df

def scrape_schedule_ncaa(year):
    '''
    Purpose: Query collegefootballdata.com and retrieve the schedule for the
        specified year (including regular season and postseason games)

    Inputs
    ------
        year : int
            Desired year for which to retreive a schedule
    
    Outputs
    -------
        df_schedule : Pandas DataFrame
            Schedule for the specified year
    '''
    df_schedule = pd.DataFrame()
    
    # retrieve regular season schedule
    response = requests.get('https://api.collegefootballdata.com/games?' + 
                            f'year={year}&seasonType=regular')
    df_reg = pd.DataFrame(json.loads(response.text))
    
    # retrieve postseason schedule
    response = requests.get('https://api.collegefootballdata.com/games?' + 
                            f'year={year}&seasonType=postseason')
    df_post = pd.DataFrame(json.loads(response.text))
    
    # combine regular and postseason schedule into one dataframe
    df_all = df_reg.append(df_post)
    
    # clean up dataframe (remove columns, standardize names & codes, etc...)
    df_all = restructure_ncaa_schedule(df_all, year)

    # write to disk
    df_all.to_csv(f'data_team/schedules/schedule_ncaa_{year}.csv', index = False)
    
    return df_schedule

def restructure_ncaa_schedule(df, year):
    '''
    Purpose: Restructure a schedule from collegefootballdata.com into the 
        desired format.

    Inputs
    ------
        df : Pandas DataFrame
            Contains the schedule of all games from a single NCAA season
        year : int
            Desired year in which to restructure a desired schedule
    
    Outputs
    -------
        df : Pandas DataFrame
            Cleaned up version of original dataframe
    '''      
    # fix problems with special characters
    df['home_team'] = df['home_team'].apply(lambda x: x.replace('�','e'))
    df['away_team'] = df['away_team'].apply(lambda x: x.replace('�','e'))
    
    # standardize team names
    df['home_team'] = rename_teams(df['home_team'], 'ncaa', 'Team')
    df['away_team'] = rename_teams(df['away_team'], 'ncaa', 'Team')
    
    # drop unnecessary columns
    df = df.drop(columns = ['id', 'season_type', 'conference_game', 'attendance',
                            'venue_id', 'home_conference', 'home_line_scores', 
                            'away_conference'])
    
    # convert date to Eastern Standard Time (US) and then to YYYYMMDD format
    df['date'] = pd.to_datetime(
            df['start_date']).dt.tz_convert('US/Eastern').dt.strftime(
                    '%Y%m%d').astype(int)
    
    # insert team codes
    df['away_code'] = rename_teams(df['away_team'], 'ncaa', 'TeamCode')
    df['home_code'] = rename_teams(df['home_team'], 'ncaa', 'TeamCode')    
    
    # remove columns that are no longer needed and reorder in desired sequence
    df = df[['season', 'week', 'date', 'home_team', 'home_points', 'home_code',
             'away_team', 'away_points', 'away_code', 'neutral_site', 'venue']]

    return df

def create_schedule_arrays(year, league):
    '''
    Purpose: Given the schedule for a college football season, create 
         dictionary that has a team name as the key and two lists as the values
         - dates : the dates of each team's games
         - opponent : the opponent code for game in a team's season

    Inputs
    ------
        year : int
            Desired year in which to restructure a desired schedule
        league : string
            Type of league to create schedule arrays for (e.g. 'nfl' or 'ncaa')
    
    Outputs
    -------
        NONE
    '''     
    if league not in ['ncaa', 'nfl']:
        print('Improper league specified. Please enter `nfl` or `ncaa`')
        return
    
    # ingest data for teams and create a lookup table for team codes
    df_teams = pd.read_csv(f'data_team/teams_{league}.csv')
    dict_team_codes = {}
    for index, row in df_teams.iterrows():
        dict_team_codes[row['Team']] = row['TeamCode']
            
    # ingest schedule data
    df = pd.read_csv(f'data_team/schedules/schedule_{league}_{year}.csv')
    
    # add a new column for checking teams involved with each game
    df['teams'] = df['home_team'] + ' ' + df['away_team']
    df = df.drop_duplicates(subset = ['teams'])
    
    # identify all unique teams in the data
    teams = sorted(list(set(list(df['home_team']) + list(df['away_team']))))
    
    dict_teams = {}
    # create a date array and opponent array for each team
    for team in teams:
        if team == 'Southern':
            continue
        # subset the full schedule to only games with the specified team
        df_team = df[df['home_team'] == team].append(
                df[df['away_team'] == team])
        
        # make a list of the dates the team has games
        list_dates = [x for x in list(df_team['date'])]
        
        # make a list of the team's opponent for each game
        list_oppts = []
        for index, row in df_team.iterrows():
            if team == row['home_team']:
                list_oppts.append(dict_team_codes[row['away_team']])
            else:
                list_oppts.append(dict_team_codes[row['home_team']])
        
        # set a requirement for 9 or more games to filter out non FBS teams
        if len(list_dates) >= 9:
            dict_teams[team] = {
                    'dates' : list_dates,
                    'opp' : list_oppts
                      }
    
    return

#==============================================================================
# Working Code
#==============================================================================

# Set the project working directory
path_dir = pathlib.Path('/home/ejreidelbach/Projects/draft-analytics-api/src/data')
os.chdir(path_dir)

# scrape the schedule for 2019 and output it to disk
for year in tqdm.tqdm(range(1999, 2020)):
    scrape_schedule_nfl(year)
    if year >= 2005:
        scrape_schedule_ncaa(year)