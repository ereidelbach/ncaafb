#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 12 12:56:56 2018

@author: ejreidelbach

:DESCRIPTION:

:REQUIRES:
   
:TODO:
    
"""
 
#==============================================================================
# Package Import
#==============================================================================
import datetime
import numpy as np
import os  
import pandas as pd
import pathlib
import re
import requests
from requests.packages.urllib3.util.retry import Retry
import tqdm
from bs4 import BeautifulSoup


#==============================================================================
# Function Definitions / Reference Variable Declaration
#==============================================================================
def soupifyURL(url):
    '''
    Purpose: Turns a specified URL into BeautifulSoup formatted HTML 

    Input: url (string): Link to the designated website to be scraped
    
    Output: soup (html): BeautifulSoup formatted HTML data stored as a
        complex tree of Python objects
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

def directoryCheck(team_name):
    '''
    Purpose: Run a check of the /Data/CFBStats/ folder to see if a folder
        exists for the specified team.  If it doesn't, create it.
        
    Input:
        - team_name (string): Name of the school being scraped
    
    Outpu:
        - NONE
    '''
    pathlib.Path('Data/CFBStats/'+team_name).mkdir(parents=True, exist_ok=True)

def restructureSchedule(df_schedule):
    '''
    Purpose: Given a Pandas DataFrame with schedule information, restructure
        the variables and/or engineer new variables out of existing variables
        until it is in the desired format.
        
        Major changes include:
            - transform a date formatted as mm/dd/yy into separate `Year`, 
                `Month`, `Day` variables
            - determine if the game was played `home` or `away`
            - determine the opponent's `AP` ranking
            - determine if the result of a game was a `W` or an `L`
            - split the result into the team's score and the opponent's score
            - calculate the point difference between the two teams
            
    Input:
        - df_schedule (DataFrame): contains schedule/gamelog information for a 
            team and associated information for a calendar season
    
    Output: 
        - df_new (DataFrame): reengineered version of the original dataframe
              into the new, desired format
    '''
    list_rows = []
    for index, row in df_schedule.iterrows():
        # Transform `Date` into `Year`, `Month`, `Day`, and `Weekday` Variables
        date = row['Date']
        row['year'] = int(datetime.datetime.strptime(date,
                 "%m/%d/%y").strftime('%Y'))
        row['month'] = datetime.datetime.strptime(date,
                 "%m/%d/%y").strftime('%B')
        row['day'] = int(datetime.datetime.strptime(date,
                 "%m/%d/%y").strftime('%e').strip())
        row['day_of_week'] = datetime.datetime.strptime(date,
                 "%m/%d/%y").strftime('%A')
        
        # Scrape where the game was played (Home or Away)
        opponent = row['Opponent']
        if opponent.split(' ')[0] == '@':
            row['home_away'] = 'Away'
            opponent = opponent.replace('@ ','')
        elif opponent.split(' ')[0] == '+':
            row['home_away'] = 'Neutral'
            opponent = opponent.replace('+ ','')
        else:
            row['home_away'] = 'Home'
        
        # Scrape the Opponent's AP ranking
        try:
            row['opp_rank'] = int(opponent.split(' ')[0])
        except:
            row['opp_rank'] = np.nan
    
        # Scrape the Opponent
        row['Opponent'] = ''.join(
                [s for s in opponent if not re.search(r'\d',s)]).strip()

        # Handle result-based variables for games that HAVE been played
        if type(row['Result']) == str:
        	# Create a column for the Team's score in each game
            row['pts_for'] = row['Result'].split(' ')[1].split('-')[0]
        
        	# Create a column for the Opponent's score in each game
            row['pts_against'] = row['Result'].split(' ')[1].split('-')[1]
        
        	# Modify the Result column to be a cimple `W` or `L` result
            row['Result'] = row['Result'][0]
                
            # Calculate the point difference between the teams
            row['pts_diff'] = float(row['pts_for']) - float(row['pts_against'])
        # Handle result-based variables for games that have NOT been played
        else:
            row['pts_for'] = np.nan
            row['pts_against'] = np.nan
            row['pts_diff'] = np.nan

        # Add the edited list to list_rows
        list_rows.append(row)
        
    # Convert list_rows to a new DataFrame
    df_new = pd.DataFrame(list_rows)
    
    # Reorder columns at the front of the DataFrame into desired order
    reorder_list_front = ['Date','Opponent','opp_rank','home_away',
                          'Result','pts_for','pts_against', 
                          'pts_diff']
    reorder_list_front.reverse() # Need to go in backwards order
    list_columns = list(df_new.columns)
    for name in reorder_list_front:
        list_columns.remove(name)
        list_columns.insert(0, name)
        
    # Reorder columns at the backof the DataFrame into desired order                
    reorder_list_back = ['month','day','year','day_of_week','Season']
    for name in reorder_list_back:
        list_columns.remove(name)
        list_columns.append(name)
    df_new = df_new[list_columns]
    
    return df_new

def scrapeTeamNames():
    '''
    Purpose: Scrapes and extracts the URL for every division FBS
        team on the CFBStats website. The team name and associated URL
        are stored in the dictionary `dict_teams` which is returned by
        this function.
        
    Input:
        - NONE
        
    Output:
        - dict_teams (dictionary): Dictionary containing all FBS teams
                and the URL to their respective team page
    '''
    # Turn the main page into BeautifulSoup HTML
    soup = soupifyURL('http://www.cfbstats.com/')
    
    # Extract the team links found on the left-hand side of the screen
    html_teams = soup.find('ul', class_='sub1')
    html_teams_clean = html_teams.findAll('li', class_='sub1')

    dict_teams = {}
    
    # Iterate over every team on the site and extract its url
    for team in html_teams_clean:
        # Use the team name as the key, the url as the value
        dict_teams[team.text] = team.find('a', href=True)['href']
        
    return dict_teams

def scrapeTeamYears(team_url):
    '''
    Purpose: Given a team, scrape the URL for every year of statistics that is
        on file for that team
    
    Input:
        - team_url (string): URL of the school's page on CFBStats.com
    
    Output:
        - dict_years (dictionary): Dictionary containing every year for which
            team statistics are recorded and the URL to those statistics
    '''
    # Turn the page into BeautifulSoup HTML
    soup = soupifyURL('http://www.cfbstats.com' + team_url)
    
    # Extract the years (and associated URLs) for the team's historic stats
    html_years = soup.find('div', {'id':'seasons'})
    html_years_clean = html_years.findAll('li')[1:] # Avoid `SELECTED` LI
    
    dict_years = {}
    
    # Add the current year to the dictionary
    dict_years[team_url.split('/')[1].split('/')[0]] = team_url
    
    # Iterate over every year and place the year-URL combo into a dictionary
    for year in html_years_clean:
        dict_years[year.text] = year.find('a', href=True)['href']
    
    return dict_years

def scrapeTeamRecords(team_name, dict_years):
    '''
    Purpose: Scrape the record's portion of a team's stats for all season
        
    Input:
        - team_name (string): Name of the school being scraped
        - dict_years (dictionary): Dictionary containing every year for which
            team statistics are recorded and the URL to those statistics
    
    Output:
        - A .csv file containing the scraped information (records.csv)
    '''
    # Create the master dictionary for storing records for all seasons/years
    dict_records = {}
    
    # Scrape each year contained in `dict_years`
    for year, url in tqdm.tqdm(dict_years.items()):
        # Create a dictionary for the year being scraped
        dict_year = {}
        
        dict_year['season'] = year
        
        # Retrieve the HTML data at the specified URL and add it to `dict_year`
        soup = soupifyURL('http://www.cfbstats.com' + url)
        table_record = soup.find('table', {'class':'team-record'})
        for row in table_record.findAll('tr')[1:]:  # ignore table header
            
            # Specify the Name of the Split being scraped (e.g. `All Games`)
            split_name = row.find('td', {'class':'split-name'}).text
            
            # Values will be stored for `Wins`, `Losses` and `Win Pct`
            # Wins
            dict_year[split_name + ' (Wins)'] = row.findAll(
                                                    'td')[1].text.split('-')[0]
            # Losses
            dict_year[split_name + ' (Losses)'] = row.findAll(
                                                    'td')[1].text.split('-')[1]
            # Win Pct
            if dict_year[split_name + ' (Losses)'] == '0': # Handle divide by 0
                dict_year[split_name + ' (Win %)'] = 0  
            else:
                dict_year[split_name + ' (Win %)'] = round(int(
                    dict_year[split_name + ' (Wins)']) / int(
                            dict_year[split_name + ' (Losses)']), 2)
                
            # Add year to master dictionary
            dict_records[year] = dict_year
    
    # Convert `dict_records` to a Pandas DataFrame
    df_records = pd.DataFrame.from_dict(dict_records, orient='index')
    
    # Standardize column names
    df_records.columns = [x.lower() for x in list(df_records.columns)]
    
    # Export the newly created Pandas DataFrame to a .csv
    df_records.to_csv('Data/CFBStats/' + team_name + '/records.csv', index=False)

def scrapeTeamSchedules(team_name, dict_years):
    '''
    Purpose: Scrape the schedule portion of a team's stats for all seasons
        
    Input:
        - team_name (string): Name of the school being scraped
        - dict_years (dictionary): Dictionary containing every year for which
            team statistics are recorded and the URL to those statistics
    
    Output:
        - A .csv file containing the scraped information (schedules.csv)
    '''
    # Create a master dataframe for storing schedule information
    df_master = pd.DataFrame()
    
    # Scrape each year contained in `dict_years`
    for year, url in tqdm.tqdm(dict_years.items()):       
        # Retrieve the HTML data at the specified URL and add it to `dict_year`
        soup = soupifyURL('http://www.cfbstats.com' + url)
       
        # Create a dataframe out of the schedule table in the Soupified HTML
        df_year = pd.read_html(str(soup.find(
                'table', {'class':'team-schedule'})))[0] 
        
        # Make column headers and remove unnecessary rows
        df_year.columns = list(df_year.iloc[0])
        df_year.drop(df_year.index[0], inplace=True)
  
        # Drop unnecessary footer
        df_year = df_year[df_year['Date'] != str(
                '@ : Away, + : Neutral Site')]

        # Add a year column to track the season the roster is for
        df_year['Season'] = int(year)

        # Restructure the Schedule Info
        df_year = restructureSchedule(df_year)
                   
        # Scrape the Game Time (hh:mm)
        df_year['game_time_hh'] = df_year['Game Time'].apply(
                lambda x: str(x).split(':')[0] if type(x) == str else np.nan)
        df_year['game_time_mm'] = df_year['Game Time'].apply(
                lambda x: str(x).split(':')[1] if type(x) == str else np.nan)
        
        # Drop old `Game Time`
        df_year.drop(['Game Time'], axis=1, inplace=True)
        
        # Reclassify variable types as numeric for certain columns
        for col in ['Attendance', 'pts_for', 'pts_against', 'game_time_hh',
                    'game_time_mm']:
            df_year[col] = df_year[col].astype(float)

        # Make all column headers lower-case for standardization
        df_year.columns = [x.lower() for x in list(df_year.columns)]
        
        # Add dataframe to master dataframe
        df_master = df_master.append(df_year)
        df_master = df_master.reset_index()                # Reset Index
        df_master.drop(['index'], axis=1, inplace=True)    # Drop Old Index
    
    # Export the newly created Pandas DataFrame to a .csv
    df_master.to_csv('Data/CFBStats/' + team_name + '/' + 'schedules.csv', 
                     index=False)

def scrapeTeamStatsCategories(team_url, type_stat):
    '''
    Purpose: Scrape the statistical categories (and associated links) 
        available for a given team on CFBStats.com.
    
    Input:
        - team_url (string): URL of the school's page on CFBStats.com
        - type_stat (string): Type of Statistical Links that need to be scraped
            (e.g. `split`, `situational`, or `game`)
    
    Outpu:
        - dict_link (Dictionary): A dictionary where the keys are the 
            statistical category and the values are the associated links
    '''
    # Identify the available split stats for the team
    dict_links = {}
            
    # Retrieve the HTML data at the specified URL
    soup = soupifyURL('http://www.cfbstats.com' + team_url)
    
    # Extract the statistical categories and associated links for the team
    link_column = soup.find('div', {'id':'leftColumn'}).find('ul')
    
    # Ignore `Team Home` and `Roster`
    for link in link_column.findAll('li')[2:]:
        # Identify the statistical category
        category = link.find('a').text.lower().replace(' ','_').replace('-','_')
        
        # Stop here if dealing only with Player Stats
        if type_stat == 'player' and category not in ['first_downs', 'penalties',
                                                      '3rd_down_conversions',
                                                      '4th_down_conversions',
                                                      'red_zone_conversions',
                                                      'turnover_margin']:
            dict_links[category + '_' + type_stat] = link.find('a')['href']
            continue
        
        # Scrape the categories HTML and identify the relevant section of links
        soup_category = soupifyURL(
                'http://www.cfbstats.com' + link.find('a')['href'])
        links_menubar = soup_category.find('div', {'id':'leftColumn'}).findAll(
                'li', {'class':'sub1'})
        
        # Check to see if sub-categories exist
        # Sub-categories Do Not Exist
        if len(links_menubar) == 2:
            # Extract the desired links based on the value of `type_stat`
            for link_sub in links_menubar:
                if link_sub.text.lower().split(' ')[0] == type_stat:
                    try:
                        dict_links[category + '_' + type_stat] = link_sub.find(
                            'a')['href']
                    except: # account for issues where site defaults to sub_stat
                        dict_links[category + '_' + type_stat] = link.find(
                            'a')['href']
        # Sub-categories Exist
        else:
            # Extract subcategories:
            sub_category_titles = soup_category.findAll(
                    'li', {'class': 'header sub1'})
            list_sub_titles = [x.text.lower() for x in sub_category_titles]
            # Extract links
            list_sub_links = []
            for link_sub in soup_category.findAll('li', {'class':'sub2'}):
                if link_sub.text.lower().split(' ')[0] == type_stat:
                    try:
                        list_sub_links.append(link_sub.find('a')['href'])
                    except: # account for issues where site defaults to sub_stat
                        list_sub_links.append(link.find('a')['href'])
            
            for header, url in zip(list_sub_titles, list_sub_links):
                dict_links[category + '_' + header + '_' + type_stat] = url

    return dict_links

def scrapeTeamStatsSplits(team_name, team_url):
    '''
    Purpose: Scrape the split statistics for every available year for
        every available statistical category
        
    Input:
        - team_name (string): Name of the school being scraped
        - team_url (string): URL of the school's page on CFBStats.com
    
    Output:
        - A .csv file containing the scraped information (xyz_split.csv)
    '''
    # Obtain Stat Category links
    dict_links = scrapeTeamStatsCategories(team_url, 'split')
        
    # Iterate through each category
    for category, link in tqdm.tqdm(dict_links.items()):
        
        # Create a master dataframe containing all the category data
        df_master = pd.DataFrame()        
        
        # Obtain links to obtain info for every year available for the team
        dict_years = scrapeTeamYears(link)
        
        # Iterate through every year available for the category
        for year, url in dict_years.items():
            
            # soupify HTML page for given stat category in given year
            soup = soupifyURL('http://www.cfbstats.com' + url)
            
            table_stat = soup.find('table', {'class':'split'})    
            
            # Convert the roster into a pandas dataframe
            df_split = pd.read_html(str(table_stat))[0]    
            
            # Make column headers and remove unncessary rows
            # Handle `Place Kicking` Multi-Indexing
            if 'place_kicking' in category:
                headers = list(df_split.iloc[1])
                headers[2:7] = ['FG_' + x for x in headers[2:7]] # Append FG
                headers[7:12] = ['XP_' + x for x in headers[7:12]] # Append XP
                df_split.columns = headers
                df_split.drop(df_split.index[0:2], inplace=True)
            # Handle all other Categories
            else:
                df_split.columns = list(df_split.iloc[0])
                df_split.drop(df_split.index[0], inplace=True)
            
            # Convert variable types to numeric
            for col in list(df_split.columns[1:]):
                # Convert rows with values of '-' to NaN
                df_split[col] = df_split[col].apply(
                        lambda x: np.nan if x == '-' else float(x))
                

            # Add a year column to track the season the roster is for
            df_split['season'] = int(year)

            # Make all column headers lower-case for standardization
            df_split.columns = [x.lower() for x in list(df_split.columns)]
            
            # Add dataframe to master dataframe
            df_master = df_master.append(df_split)
            df_master = df_master.reset_index()                 # Reset Index
            df_master.drop(['index'], axis=1, inplace=True)     # Drop Old Index
            
            # Export the newly created Pandas DataFrame to a .csv
            df_master.to_csv(
                    'Data/CFBStats/' + team_name + '/' + category + '.csv', 
                    index=False)

def scrapeTeamStatsSituational(team_name, team_url):
    '''
    Purpose: Scrape the statistical game logs for every available year for
        every available statistical category
        
    Input:
        - team_name (string): Name of the school being scraped
        - team_url (string): URL of the school's page on CFBStats.com
    
    Output:
        - A .csv file containing the scraped information (xyz_situational.csv)
    '''
    # Obtain Stat Category links
    dict_links = scrapeTeamStatsCategories(team_url, 'situational')
        
    # Iterate through each category
    for category, link in tqdm.tqdm(dict_links.items()):
        
        # Create a master dataframe containing all the category data
        df_master = pd.DataFrame()       
        
        # Obtain links to obtain info for every year available for the team
        dict_years = scrapeTeamYears(link)

        # Iterate through every year available for the category
        for year, url in dict_years.items():
            
            # soupify HTML page for given stat category in given year
            soup = soupifyURL('http://www.cfbstats.com' + url)
            table_stat = soup.find('table', {'class':'situational'})    

            # Convert the roster into a pandas dataframe
            df_situational = pd.read_html(str(table_stat))[0]    
                       
            # Make column headers and remove unncessary rows
            df_situational.columns = list(df_situational.iloc[0])
            df_situational.drop(df_situational.index[0], inplace=True)
            
            # Convert variable types to numeric
            for col in list(df_situational.columns[1:]):
                # Convert rows with values of '-' to NaN
                df_situational[col] = df_situational[col].apply(
                        lambda x: np.nan if x == '-' else float(x))
                

            # Add a year column to track the season the roster is for
            df_situational['season'] = int(year)
            
            # Drop unnecessary footer
            df_situational = df_situational[df_situational['Situation'] != str(
                    '1st: First Downs; 15+: Pass completions of 15 or more ' + 
                    'yards; 25+: Pass completions of 25 or more yards')]

            # Make all column headers lower-case for standardization
            df_situational.columns = [x.lower() for x in list(
                    df_situational.columns)]
         
            # Add dataframe to master dataframe
            df_master = df_master.append(df_situational)
            df_master = df_master.reset_index()                # Reset Index
            df_master.drop(['index'], axis=1, inplace=True)    # Drop Old Index
            
            # Export the newly created Pandas DataFrame to a .csv
            df_master.to_csv(
                    'Data/CFBStats/' + team_name + '/' + category + '.csv', 
                    index=False)

def scrapeTeamStatsGameLogs(team_name, team_url):
    '''
    Purpose: Scrape the statistical game logs for every available year for
        every available statistical category
        
    Input:
        - team_name (string): Name of the school being scraped
        - team_url (string): URL of the school's page on CFBStats.com
    
    Output:
        - A .csv file containing the scraped information (xyz_game.csv)
    '''
    # Obtain Stat Category links
    dict_links = scrapeTeamStatsCategories(team_url, 'game')
        
    # Iterate through each category
    for category, link in tqdm.tqdm(dict_links.items()):
        
        # Create a master dataframe containing all the category data
        df_master = pd.DataFrame()        
        
        # Obtain links to obtain info for every year available for the team
        dict_years = scrapeTeamYears(link)

        # Iterate through every year available for the category
        for year, url in dict_years.items():       
            
            # soupify HTML page for given stat category in given year
            soup = soupifyURL('http://www.cfbstats.com' + url)
            table_stat = soup.find('table', {'class':'game-log'})    

            # Convert the roster into a pandas dataframe
            df_game = pd.read_html(str(table_stat))[0]    
                       
            # Make column headers and remove unncessary rows
            # Handle `Place Kicking` Multi-Indexing
            if 'place_kicking' in category:
                headers = list(df_game.iloc[1])
                headers[4:7] = ['FG_' + x for x in headers[4:7]] # Append FG
                headers[7:10] = ['XP_' + x for x in headers[7:10]] # Append XP
                df_game.columns = headers
                df_game.drop(df_game.index[0:2], inplace=True)
            # Handle all other Categories
            else:
                df_game.columns = list(df_game.iloc[0])
                df_game.drop(df_game.index[0], inplace=True)

            # Drop unnecessary footer
            df_game = df_game[df_game['Date'] != str(
                    '@ : Away, + : Neutral Site')]
    
            # Account for `Totals` Row
            df_game.drop(df_game.index[-1], inplace=True)
            
            # Convert variable types to numeric for every column after `Result`
            for col in list(df_game.columns)[
                    list(df_game.columns).index('Result')+1:]:
                # Convert rows with values of '-' to NaN
                df_game[col] = df_game[col].apply(
                        lambda x: np.nan if x == '-' else float(x))

            # Add a year column to track the season the roster is for
            df_game['Season'] = int(year)

            # Restructure schedule variables into desired format
            df_game = restructureSchedule(df_game)
            
            # Make all column headers lower-case for standardization
            df_game.columns = [x.lower() for x in list(df_game.columns)]
          
            # Add dataframe to master dataframe
            df_master = df_master.append(df_game)
            df_master = df_master.reset_index()                # Reset Index
            df_master.drop(['index'], axis=1, inplace=True)    # Drop Old Index
            
            # Export the newly created Pandas DataFrame to a .csv
            df_master.to_csv(
                    'Data/CFBStats/' + team_name + '/' + category + '.csv', 
                    index=False)

def scrapeTeamRosters(team_name, dict_years):
    '''
    Purpose: Scrape the roster for every available year
        
    Input:
        - team_name (string): Name of the school being scraped
        - dict_years (dictionary): Dictionary containing every year for which
            team statistics are recorded and the URL to those statistics
    
    Output:
        - A .csv file containing the scraped information (Rosters.csv)
    '''
    # Create the master dictionary for storing schedules for all seasons/years
    df_rosters = pd.DataFrame()
    
    # Scrape each year contained in `dict_years`
    for year, url in tqdm.tqdm(dict_years.items()):

        # Create the roster URL
        url_roster = url.split('index.html')[0] + 'roster.html'
        
        # Retrieve the HTML data at the specified URL and add it to `dict_year`
        soup = soupifyURL('http://www.cfbstats.com' + url_roster)
        table_roster = soup.find('table', {'class':'team-roster'})    
        
        # Convert the roster into a pandas dataframe
        df_year = pd.read_html(str(table_roster))[0]
        
        # Make the first row the column names, then remove the first row
        df_year.columns = list(df_year.iloc[0])
        df_year.drop(df_year.index[0], inplace=True)
        
        # Add a year column to track the season the roster is for
        df_year['season'] = int(year)
        
        # Append the year DataFrame to the master DataFrame
        df_rosters = df_rosters.append(df_year)
        
    # Create a First Name Variable
    df_rosters['name_first'] = df_rosters['Name'].apply(
            lambda x: extractNameFirst(x))

    # Create a Last Name Variable
    df_rosters['name_last'] = df_rosters['Name'].apply(
            lambda x: extractNameLast(x))
            
    # Create a Height (Inches) Variable
    df_rosters['height_inches'] = df_rosters['Ht'].apply(
            lambda x: extractHeightInches(x))
    
    # Create a Hometown (City) Variable
    df_rosters['city'] = df_rosters['Hometown'].apply(lambda x: x.split(', ')[0])
    
    # Create a Hometown (State) Variable
    def toState(hometown):
        if hometown == '-':
            return hometown
        elif len(hometown.split(', ')) == 1:
            return hometown
        else:
            return(hometown.split(', ')[1])                
    df_rosters['state'] = df_rosters['Hometown'].apply(lambda x: toState(x))

    
    # Remove unnecessary columns, rename columns, and reorder columns
    df_rosters.drop(['Name', 'Hometown'], axis = 1, inplace=True)
    df_rosters.rename(columns = {'No':'number', 'Pos':'position', 'Yr':'year',
                                 'Ht':'height', 'Wt':'weight', 
                                 'Last School':'prev_school'}, inplace=True)
    df_rosters = df_rosters[['season', 'name_last', 'name_first', 'number', 
                             'position', 'year', 'height', 'height_inches', 
                             'weight', 'city', 'state',
                             'prev_school']]
    
    # Reformat the weight column to a numeric value
    df_rosters['weight'] = df_rosters['weight'].apply(
            lambda x: int(x) if x != '-' else np.nan)

    # Export the newly created Pandas DataFrame to a .csv
    df_rosters.to_csv('Data/CFBStats/' + team_name + '/rosters.csv', index=False)

def scrapeTeamStats(team_name, team_url):
    '''
    Purpose: Direct the scraping all the statistical information for all
        college teams on CFBStats.com
        
    Input:
        - team_name (string): Name of the school being scraped
        - team_url (string): URL of the school's page on CFBStats.com
    
    Output:
        - None
    '''
    # Check to see if the team's directory exists (and if not -> make it)
    directoryCheck(team_name)
    
    # Obtain links to obtain info for every year available for the team
    dict_years = scrapeTeamYears(team_url)
       
    # Scrape Record (i.e. wins-losses) for every year 
    scrapeTeamRecords(team_name, dict_years)
    
    # Scrape Schedule information for every year
    scrapeTeamSchedules(team_name, dict_years)

    # Scrape Rosters for every year
    scrapeTeamRosters(team_name, dict_years)
    
    # Scrape Split Statistics for every year
    scrapeTeamStatsSplits(team_name, team_url)

    # Scrape Situational Statistics for every year
    scrapeTeamStatsSituational(team_name, team_url)

    # Scrape Statistical Game Logs for every year
    scrapeTeamStatsGameLogs(team_name, team_url)
    
    # Provide a status update
    print('Done with: ' + team_name)


def scrapePlayerStats(team_name, team_url):
    '''
    Purpose: Scrape the player statistics for every available year for
        every available statistical category
        
    Input:
        - team_name (string): Name of the school being scraped
        - team_url (string): URL of the school's page on CFBStats.com
    
    Output:
        - A .csv file containing the scraped information (xyz_playerstats.csv)
    '''
    # Obtain Stat Category links
    dict_links = scrapeTeamStatsCategories(team_url, 'player')
        
    # Iterate through each category
    for category, link in tqdm.tqdm(dict_links.items()):
        
        # Create a master dataframe containing all the category data
        df_master = pd.DataFrame()        
        
        # Obtain links to obtain info for every year available for the team
        dict_years = scrapeTeamYears(link)
        
        # Iterate through every year available for the category
        for year, url in dict_years.items():
            
            # soupify HTML page for given stat category in given year
            soup = soupifyURL('http://www.cfbstats.com' + url)
            
            table_stat = soup.find('table', {'class':'leaders'})    
            
            # Convert the roster into a pandas dataframe
            df_players = pd.read_html(str(table_stat))[0]    
            
            # Remove the first column as it is a redundant index
            df_players.drop([0], axis=1, inplace = True)
            
            # Make column headers and remove unncessary rows
            # Handle `Place Kicking` Multi-Indexing
            if 'place_kicking' in category:
                headers = list(df_players.iloc[1])
                headers[4:9] = ['FG_' + x for x in headers[4:9]] # Append FG
                headers[9:14] = ['XP_' + x for x in headers[9:14]] # Append XP
                df_players.columns = headers
                df_players.drop(df_players.index[0:2], inplace=True)
            # Handle all other Categories
            else:
                df_players.columns = list(df_players.iloc[0])
                df_players.drop(df_players.index[0], inplace=True)
            
            # Convert variable types to numeric (after `Name`, `Yr`, and `Pos`)
            for col in list(df_players.columns[3:]):
                # Convert rows with values of '-' to NaN
                df_players[col] = df_players[col].apply(
                        lambda x: np.nan if x == '-' else float(x))

            # Add a year column to track the season the roster is for
            df_players['season'] = int(year)

            # Make all column headers lower-case for standardization
            df_players.columns = [x.lower() for x in list(df_players.columns)]
            
            # Delete `Total` and `Opponents` rows (Last two rows of DataFrame)
            df_players.drop(df_players.index[-2:], inplace=True)
            
            # Delete any rows where the player's name is `Team`
            df_players = df_players[df_players['name'] != 'Team']
            
            # Add dataframe to master dataframe
            df_master = df_master.append(df_players)
            df_master = df_master.reset_index()                 # Reset Index
            df_master.drop(['index'], axis=1, inplace=True)     # Drop Old Index
            
            # Export the newly created Pandas DataFrame to a .csv
            df_master.to_csv(
                    'Data/CFBStats/' + team_name + '/' + category + '.csv', 
                    index=False)    

def aggregateRosters():
    '''
    Purpose: Ingest roster information for all teams and combine into one 
        massive roster file
        
    Input:
        - NONE
        
    Output
        - NONE
    '''
    # Set the directory where the files are located
    dir_path = pathlib.Path('/home/ejreidelbach/Projects/CollegeFootball/Data/CFBStats')
    
    # Find every folder in the Data directory
    list_files_rosters = list(dir_path.rglob('rosters.csv'))
    
    df_master = pd.DataFrame()
    # Iterate over every folder and compile all roster into df_master
    for file in list_files_rosters:
        df = pd.read_csv(file)
        df['school'] = str(file).split('CFBStats/')[1].split('/rosters.csv')[0]
        df_master = df_master.append(df)
        print('Done with: ' +str(file))
    df_master = df_master.reset_index()                 # Reset Index
    df_master.drop(['index'], axis=1, inplace=True)     # Drop Old Index
    df_master.to_csv(
            dir_path.joinpath(pathlib.Path(
                    'merged_rosters.csv')), index=False) # Export to CSV    

def cleanRoster(roster):
    '''
    Purpose: Clean up errors in a team's roster. Such errors include missing 
        values (e.g. roster numbers, position, hometown, etc.).  In addition,
        create the redshirt variables: redshirt_yr (boolean indicator if a 
        specific year is a player's redshirt year) and redshirted (boolean
        indicator if a player redshirted in their career)
        
    Input:
        - roster (DataFrame): Table containing a team's roster information
        
    Output:
        - roster_clean (DataFrame): Table with missing values filled in and
            redshirt variables included
    '''
    
def extractNameFirst(name):
    '''
    Purpose: Extract the first name from a player's name formatted as 
        `Last Name, First Name`
        first and last name variables (accounting for suffixes such as Jr./Sr.)
    
    Input:
        - name (string): Player's name in the format `Last name, First name`
    
    Output:
        - (string): The player's first name
    '''
    list_split_name = name.split(', ')   # split name on `, `
    
    if len(list_split_name) == 1:       # handle 1-word names
        return('')
    elif len(list_split_name) == 2:     # handle 2-word names
        return(list_split_name[1])
    else:                               # handle 3-word names
        return(list_split_name[2])

def extractNameLast(name):
    '''
    Purpose: Extract the last name from a player's name formatted as 
        `Last Name, First Name` (accounting for suffixes such as Jr./Sr.)
    
    Input:
        - name (string): Player's name in the format `Last name, First name`
    
    Output:
        - (string): The player's last name (to include suffixes)
    '''
    list_split_name = name.split(', ')   # split name on `, `
    
    if len(list_split_name) == 1:       # handle 1-word names
        return(name)
    elif len(list_split_name) == 2:     # handle 2-word names
        return(list_split_name[0])
    else:                               # handle 3-word names
        return(list_split_name[0] + ' ' + list_split_name[1])
            
def extractHeightInches(height):
    '''
    Purpose: Convert a height variable formatted in `Feet - Inches` into
        a variable of inches only (i.e. feet * 12 inches + inches)
        
    Input:
        - height (string): Height of player formatted in `Feet - Inches`
    
    Output:
        - (int): Height of player in total inches
    '''        
    if height == '-':
        return(np.nan)
    else:
        heightFeet = int(height.split('-')[0])
        heightInches = int(height.split('-')[1])
        
        return(heightFeet*12 + heightInches)
    
#==============================================================================
# Working Code
#==============================================================================

# Set the project working directory
os.chdir(r'/home/ejreidelbach/Projects/CollegeFootball')

# Start a headless browser of Firefox 
#browser = initiateBrowser('http://www.cfbstats.com/', True)

# Create a dictionary of team names and links
dict_teams = scrapeTeamNames()

# Scrape the stats for each team (creating CSV files along the way)
for team_name, team_url in dict_teams.items():
#list_teams = list(dict_teams.keys())
#for team_name in list_teams[list_teams.index('Oregon'):]:
#    team_url = dict_teams[team_name]
    #scrapeTeamStats(team_name, team_url)
        
#    # Check to see if the team's directory exists (and if not -> make it)
#    directoryCheck(team_name)
#    
#    # Obtain links to obtain info for every year available for the team
#    dict_years = scrapeTeamYears(team_url)
       
#    # Scrape Record (i.e. wins-losses) for every year 
#    scrapeTeamRecords(team_name, dict_years)
    
#    # Scrape Schedule information for every year
#    scrapeTeamSchedules(team_name, dict_years)

#    # Scrape Rosters for every year
#    scrapeTeamRosters(team_name, dict_years)
    
#    # Scrape Split Statistics for every year
#    scrapeTeamStatsSplits(team_name, team_url)
    
#    # Scrape Situational Statistics for every year  
#    scrapeTeamStatsSituational(team_name, team_url)

#    # Scrape Statistical Game Logs for every year
#    scrapeTeamStatsGameLogs(team_name, team_url)
    
    # Scrape Player Statistics for every category for every year
    scrapePlayerStats(team_name, team_url)
    
    # Provide a status update
    print('Done with: ' + team_name)