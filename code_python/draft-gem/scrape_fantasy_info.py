#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 26 16:02:43 2019

@author: ejreidelbach

:DESCRIPTION:
    Scrape rotoguru1.com to acquire fantasy information for Fan Duel, Draft 
    Kings and Yahoo to obtain salary and points data for players for as many
    years as possible.
    
    URL:
    http://rotoguru1.com/cgi-bin/fyday.pl?week=17&year=2018&game=fd

:REQUIRES:
    Refer to the Package Import section of the script
    
:TODO:
    TBD
"""
 
#==============================================================================
# Package Import
#==============================================================================
import datetime
import glob
import io    
import os  
import pandas as pd
import pathlib
import string
import requests
import tqdm

from bs4 import BeautifulSoup
from urllib3.util import Retry
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

#==============================================================================
# Reference Variable Declaration
#==============================================================================
nfl_teams = ['Arizona',
             'Atlanta',
             'Baltimore',
             'Buffalo',
             'Carolina',
             'Chicago',
             'Cincinnati',
             'Cleveland',
             'Dallas',
             'Denver',
             'Detroit',
             'Green Bay',
             'Houston',
             'Indianapolis',
             'Jacksonville',
             'Kansas City',
             'LA Chargers',
             'LA Rams',
             'Miami',
             'Minnesota',
             'New England',
             'New Orleans',
             'New York G',
             'New York J',
             'Oakland',
             'Philadelphia',
             'Pittsburgh',
             'San Francisco',
             'Seattle',
             'Tampa Bay',
             'Tennessee',
             'Washington']
#==============================================================================
# Function Definitions
#==============================================================================
def rename_nfl(name_series):
    '''
    Purpose: Rename an NFL team to a standardized name as specified in 
        the file `teams_nfl.csv`

    Inputs
    ------
        name_series : Pandas Series
            Contains a NFL team names that requires standardization
    
    Outputs
    -------
        name_series_std : Pandas Series
            Contains a NFL team names that have been standardized
    '''  
    # read in school name information
    df_team_names = pd.read_csv('data_team/teams_nfl.csv')
     
    # convert the dataframe to a dictionary such that the keys are the
    #   optional spelling of each team and the value is the standardized
    #   name of the team
    dict_team_names = {}
    
    for index, row in df_team_names.iterrows():
        # isolate the alternative name columns
        names = row[[x for x in row.index if ('name' in x.lower() and (
                'url' not in x.lower()))]]
        # convert the row to a list that doesn't include NaN values
        list_names_nicknames = [
                x for x in names.values.tolist() if str(x) != 'nan']
        # add the abbreviated name
        list_names_nicknames.append(row['Team'])
        # set the standardized name to be used
        name_standardized = row['Team']
        # for every alternative spelling of the team, set the value to be
        #   the standardized name
        for name_alternate in list_names_nicknames:
            dict_team_names[name_alternate.lower()] = name_standardized
            
    def swapteamName(name_old):
        if ((name_old == 'nan') or (pd.isna(name_old)) or 
             (name_old == 'none') or (name_old == '')):
            return ''
        try:
            return dict_team_names[name_old]
        except:
            print('Did not find: %s' % (name_old))
            return name_old
            
    name_series_std = name_series.apply(lambda x: swapteamName(x.lower()))
    
    return name_series_std

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

def scrape_nfl_schedules(year_start, year_end):
    '''
    Purpose: Scrape the dates of NFL games in each season for the specified years
    
    Inputs
    ------
        year_start : int
            Year to begin scraping data
        year_end : int
            Year to stop scraping data
            
    Outputs
    -------
        NONE    
    '''
    for year in tqdm.tqdm(range(year_start, year_end + 1)):
        # specify the url and retrieve the Beautiful Soup version of the site
        url = f'https://www.pro-football-reference.com/years/{year}/games.htm'
        soup = soupify_url(url)
        
        # extract the schedule table and convert it to a pandas dataframe
        table = soup.find('table', {'id':'games'})
        df_year = pd.read_html(str(table))[0]
        
        # remove header rows from the table
        df_year = df_year[df_year['Week'] != 'Week']
    
        # isolate the data we're interested in (Week and Date)
        df_year = df_year[['Week', 'Date']]
        df_year = df_year.drop_duplicates()
        df_year = df_year.reset_index(drop = True)
        
        # isolate 'Playoffs' header row and remove all rows after that
        if 'Playoffs' in df_year['Date'].value_counts():
            df_year = df_year.iloc[0:df_year[df_year['Date'] == 'Playoffs'].index[0]]
        
        # iterate over every week in the schedule and extract the calendar date
        list_dates = []
        for index, row in df_year.iterrows():
            if 'January' not in row['Date']:
                date = row['Date'] + ', %i' % year
            else:
                date = row['Date'] + ', %i' % (year + 1)
            list_dates.append(date)
        df_year['Date'] = list_dates
        
        # break the year out into separate variables for ease of reference
        df_year['Month'] = df_year['Date'].apply(
                lambda x: int(datetime.datetime.strptime(
                        x, '%B %d, %Y').strftime('%m')))
        df_year['Day'] = df_year['Date'].apply(
                lambda x: int(datetime.datetime.strptime(
                        x, '%B %d, %Y').strftime('%d')))
        df_year['Year'] = df_year['Date'].apply(
                lambda x: int(datetime.datetime.strptime(
                        x, '%B %d, %Y').strftime('%Y')))
        df_year['Weekday'] = df_year['Date'].apply(
                lambda x: datetime.datetime.strptime(
                        x, '%B %d, %Y').strftime('%A'))
#        df_year = df_year[df_year['Weekday'] == 'Sunday']
        
        # write datest to disk
        df_year.to_csv(f'data_fantasy/nfl_schedule_{year}.csv', index = False)
    
    return

def scrape_rotoguru_all(year = ''):
    '''
    Purpose: Scrape player salary and points information for all available 
        years and fantasy sites (i.e. Draft Kings, Fan Duel and Yahoo)
        
        Note: Does not contain projected points (only historical reslts)

    Inputs
    ------
        year : int
            nfl season to be scraped
            Note: if no year value present, all available years will be scraped
    
    Outputs
    -------
        NONE
    '''
    if year == '':
        year_start = 2011
        year_end = 2020
    else:
        year_start = year
        year_end = year + 1
        
    # Iterate over every available year
    for year in range(year_start, year_end):
        # initialize empty dataframes for storing yearly data for each service
        df_dk = pd.DataFrame()
        df_yh = pd.DataFrame()
        df_fd = pd.DataFrame()

        print('###------ Starting Year: %i ------###' % year)
        
        # scrape all 17 weeks of data
        for week in tqdm.tqdm(range(1, 18)): 
            # scrape Fan Duel (goes back to 2011)
            df_fd_week = scrape_rotoguru_week(year, week, 'fd')
            # handle week 1
            if len(df_fd) == 0:
                df_fd = df_fd_week.copy()
            # handle all weeks 2-17
            else:
                df_fd = df_fd.append(df_fd_week)

            # scrape Draft Kings (goes back to 2011)            
            if year >= 2014:
                df_dk_week = scrape_rotoguru_week(year, week, 'dk')
                # handle all weeks 1
                if len(df_dk) == 0:
                    df_dk = df_dk_week.copy()
                # handle all weeks 2-17
                else:
                    df_dk = df_dk.append(df_dk_week)

            # scrape Yahoo (goes back to 2017)                
            if year >= 2017:
                df_yh_week = scrape_rotoguru_week(year, week, 'yh')
                # handle all weeks 1
                if len(df_yh) == 0:
                    df_yh = df_yh_week.copy()
                # handle all weeks 2-17
                else:
                    df_yh = df_yh.append(df_yh_week)
        
        # write draft kings data to disk
        if len(df_dk) > 0:
            df_dk.to_csv(f'data_fantasy/draftkings_{year}.csv', index = False)
        # write fan duel data to disk
        if len(df_fd) > 0:
            df_fd.to_csv(f'data_fantasy/fanduel_{year}.csv', index = False)
        # write yahoo data to disk
        if len(df_yh) > 0:
            df_yh.to_csv(f'data_fantasy/yahoo_{year}.csv', index = False)
                
    return

def scrape_rotoguru_week(year, week, service):
    '''
    Purpose: Scrape player salary and points information for a specific year 
        and week for all available fantasy sites (i.e. Draft Kings, Fan Duel 
        and Yahoo)

    Inputs
    ------
        year : int
            nfl season to be scraped
        week : int
            week of the season to be scraped
        service : string
            Fantasy service to scrape data for
                dk : Draft Kings
                fd : Fan Duel
                yh : Yahoo
    
    Outputs
    -------
        NONE
    '''
    # create the url based on the function inputs
    url = (f'http://rotoguru1.com/cgi-bin/fyday.pl?week={week}&' + 
            f'year={year}&game={service}&scsv=1')
    
    # retrieve the beautiful soup formatted version of the site
    soup = soupify_url(url)

    # load table data into a pandas dataframe and return it
    df = pd.read_csv(io.StringIO(soup.find("pre").text), sep = ";")
   
    # remove players who are on a bye week    
    df = df[df['Oppt'] != '-']
    # remove rows that have missing data
    df = df.dropna()
    
    # reformat name variable
    list_names = []
    for name in list(df['Name']):
        # remove the word 'Defense' from team names
        if ('Defense' in name) or (name in nfl_teams):
            list_names.append(name.replace(' Defense', ''))
        # swap the position of a player's last name and first name
        else:
            list_names.append(name.split(', ')[1] + ' ' + name.split(', ')[0])
    df['Name'] = list_names
    
    # standardize team names for player teams
    df['Team'] = rename_nfl(df['Team'])
    # standardize team names for player opponents
    df['Oppt'] = rename_nfl(df['Oppt'])
    # rename variables to desired scheme
    df = df.rename(columns = {'Pos':'Position',
                              'Oppt':'Opponent',
                              '%s points' % service.upper():'Actual',
                              '%s salary' % service.upper():'Salary',
                              'h/a':'Home/Away'})
    
    # reorder variables as desired
    df = df[['Week', 'Year', 'Name', 'Position', 'Team', 
             'Home/Away', 'Opponent', 'Actual', 'Salary']]
    
    return df

def scrape_historical_projections(year):
    '''
    Purpose: Scrape project player points from a variety of fantasy services
        for a given year

    Inputs
    ------
        year : int
            nfl season to be scraped

    Outputs
    -------
        NONE
    '''
    # read in schedule data for the given year
    df_schedule = pd.read_csv(f'data_fantasy/nfl_schedule_{year}.csv')
    # only keep Sundays (the data is only set once per week)
    df_schedule = df_schedule[df_schedule['Weekday'] == 'Sunday']
    
    options = Options()
    options.headless = True
    browser = webdriver.Firefox(options=options)
    
    # iterate over every fantasy service we have data for
    for service in ['draftkings', 'fanduel', 'yahoo']:
        df_service = pd.DataFrame()
        for index, row in tqdm.tqdm(df_schedule.iterrows()):
            # limit the scope of the schedule to the next week of games
            if (datetime.datetime.today() + datetime.timedelta(days = 7)) < (
                    datetime.datetime.strptime(row['Date'], '%B %d, %Y')):
                continue
            
            # otherwise extract date info
            row_week = row['Week']
            row_year = row['Year']
            row_month = str(row['Month']).zfill(2)
            row_day = str(row['Day']).zfill(2)
            
            # iterate over every position group
            for position in tqdm.tqdm(['qb', 'rb', 'wr', 'te', 'defense', 'kicker']):
                # set the URL to be scraped
                url = (f'https://rotogrinders.com/projected-stats/nfl-{position}' + 
                       f'?site={service}&date={row_year}-{row_month}-{row_day}')
        
                # retrieve beautiful-soup version of the data        
                browser.get(url)
                browser.implicitly_wait(10)
                
                html = browser.page_source
                soup = BeautifulSoup(html, 'html.parser')
                
                # attempt to extract projected points data (if it exists)
                try:
                    # isolate the table data
                    dict_table = {}
                    table = soup.find('div', {'id':'proj-stats'})
                    columns = table.find_all('div', {'class':'rgt-col'})
                    # iterate over every column and extra data row by row
                    for col in columns:
                        # test for a column of interest
                        header = col.find('div', {'class':'rgt-hdr'}).text
                        if header in ['Name', 'Salary', 'Team', 'Position', 
                                      'Opp', 'Ceil', 'Floor', 'Points', 'Pt/$/K']:
                            list_rows = []
                            for row in col.find_all('div')[1:]:
                                list_rows.append(row.text)
                            dict_table[header] = list_rows
                            
                    # add to dataframe
                    if len(df_service) == 0:
                        df_service = pd.DataFrame.from_dict(dict_table)
                        df_service['Week'] = row_week
                    else:
                        df_week = pd.DataFrame.from_dict(dict_table)
                        df_week['Week'] = row_week
                        df_service = df_service.append(df_week, sort = True)
                except:
                    pass
                
        # standardize player's team
        df_service['Team'] = rename_nfl(df_service['Team'])
        # remove away status from player's opponent
        df_service['Opp'] = df_service['Opp'].apply(lambda x: x.replace('@ ',''))
        # standardize player's opponent team
        df_service['Opp'] = rename_nfl(df_service['Opp'])
        # rename opponent variable
        df_service = df_service.rename(columns = {'Opp':'Opponent'})
        # insert year variable
        df_service['Year'] = year
        
        # reorder variables as desired
        df_service = df_service[['Week', 'Year', 'Name', 'Position', 'Team', 
                                 'Opponent', 'Points', 'Salary', 'Floor', 'Ceil',
                                 'Pt/$/K']]        
        
        # write to disk
        df_service.to_csv(f'data_fantasy/predictions_{service}_{row_month}' + 
                          f'-{row_day}-{year}.csv', index = False)
            
    browser.quit()
    
def merge_historical_with_actual(year):
    '''
    Purpose: Merge historical fantasy projections with actual results for the
        specified year

    Inputs
    ------
        year : int
            nfl season to be scraped

    Outputs
    -------
        NONE
    '''
    for service in ['draftkings', 'fanduel', 'yahoo']:
        # determine the filename for the given service
        filename = glob.glob(f'data_fantasy/predictions_{service}*.csv')[0]
        # read in predicted points
        df_predict = pd.read_csv(filename)
        # drop unnecessary columns
        df_predict = df_predict.drop(columns = ['Salary', 'Pt/$/K'])
        # read in actual points scored
        df_actual = pd.read_csv(f'data_fantasy/{service}_{year}.csv')
        
        # merge predicted and actual points
        df_merge = pd.merge(df_actual,
                            df_predict,
                            how = 'left',
                            on = ['Name', 'Opponent', 'Position', 'Team', 'Week'])
        # rename columns to clarify variables
        df_merge.rename(columns = {'Points':'Predicted',
                                   'Ceil':'Predicted_Ceiling',
                                   'Floor':'Predicted_Floor'}, inplace = True)
        # write to disk
        df_merge.to_csv(f'data_fantasy/{service}_{year}.csv', index = False)
    
#==============================================================================
# Working Code
#==============================================================================

scrape_historical_projections(2019)
scrape_rotoguru_all(2019)
merge_historical_with_actual(2019)