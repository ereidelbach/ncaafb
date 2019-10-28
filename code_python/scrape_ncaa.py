#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Nov  3 12:22:28 2017

@author: ejreidelbach
"""

# Import the Required Libraries
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os

###############################################################################
# Function Definitions

def team_info(year):
    ''' This function will scrape the NCAA College Football stats website 
        and return basic information for a team that will be needed for 
        subsequent scraping calls (i.e. components of specific web URLs)
        
        Input:
            year (string) - the yea rof the season being requested
            
        Output:
            A list in which each entry contains four values:
                - University Name (string)
                - ID number of the University (string)
                - ID number of the year (string)
                - Year being queried  (string)
    '''

    # Define a user agent so that they site doesn't forbid us from scraping data.
    headers = {"User-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"}
    
    # Specify the URL to be scraped based on the year that the user submits
    url = "http://stats.ncaa.org/team/inst_team_list?academic_year=" + year + "&conf_id=-1&division=11&sport_code=MFB"

    # Make the request to the website and soupify the data
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.content)
    
    # Pull out team names
    ## The names are included in 1 massive block and 3 smaller columns
    ##   We pull out the 3 smaller columns since it's easier to work with
    rows = soup.find_all("table")[1:4]
    
    ## For every row in the 3 columns, extract the Team, teamID, and yearID,
    ##   then add them to a list, and convert that list to a dataframe
    ##   once all rows have been read.  Return that dataframe.
    team_list = []
    for tr in rows:
        cols = tr.findAll('td')
        for td in cols:
            team_list.append([td.a.text, str(td.a).split('/')[2], str(td.a).split('"')[1].split('/')[3], year])

    print("Complete with year: " + year)
    
    return team_list 
  
def team_roster(team):
    ''' This functions will scrape the NCAA College Football stats website
        and create a list that contains a team's entire roster for the
        specified year.
        
        Input:
            team (list) - a list containing all team information from a specific year.
                - team:    university name (string)
                - teamID:  ID number of the university (string)
                - yearID:  ID number of the year being queried (string)
                - year:    year being queried (string)
            
        Output:
            A list in which each entry (i.e. a player) contains 7 values:
                - jersey (string): player's jersey number
                - player (string): player's name in last name, first name format
                - pos (string):    player's position (1 or 2 letter code)
                - yr (string):     player's academic year (1 or 2 letter code)
                - gp (string):     player's number of games played in the season
                - gs (string):     player's number of games started in the season  
                - team (string):   team for which the player plays
                - year (string):   year of this player's information
    '''

    # Define a user agent so that they site doesn't forbid us from scraping data.
    headers = {"User-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"}
    
    # Specify the URL to be scraped based on the year that the user submits
    url = "http://stats.ncaa.org/team/" + team[1] + "/roster/" + team[2]

    # Make the request to the website and soupify the data
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.content)

    # Pull out rows of players
    table = soup.find('tbody')
    rows = table.findAll('tr')
    
    # For every players, extract their information [player_list] and add the
    #   completed list to the team roster [team_list]
    team_list = []
    for tr in rows:
        player_list = []
        cols = tr.findAll('td')
        for td in cols:
            player_list.append(td.text)
        player_list.append(team[3])     # add in the year of the roster
        player_list.append(team[0])     # add in the team of the roster
        team_list.append(player_list)
    
    return team_list
   
def team_stats(year):
    ''' This functions will scrape the NCAA College Football stats website
        and create a dataframe that contains a team's statistics for the specified 
        year.
        
        Input:
            - year:    year being queried (string)
            
        Output:
            A list of lists where each list represents team stats for a particular
            week.
            
            There are 47 statistical categories for which stats will be retrieved.
    '''
    # Define a user agent so that they site doesn't forbid us from scraping data.
    headers = {"User-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"}
    
    # Specify the URL to be scraped based on the year that the user submits
    url = "http://stats.ncaa.org/rankings?academic_year=" + str(year) + "&division=11.0&sport_code=MFB"
    
    # Make the request to the website and soupify the data
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.content)
    
    ### Determine the links for all the statistical categories
    table = soup.find('select', id="Stats")
    cats = table.findAll('option')
    cats_dict = {}
    for cat in cats:
        if cat['value'] != "":
            cats_dict[cat.text] = cat['value']
            
    # Access each individual statistical page and retrieve the statistics
    full_stats_list = []
    for cat in cats_dict:
        print("Pulling category: " + cat)
        url = "http://stats.ncaa.org/rankings?academic_year=" + str(year) + "&division=11.0&sport_code=MFB&stat_seq=" + cats_dict[cat]
        r = requests.get(url, headers=headers)
        soup = BeautifulSoup(r.content)
        
        # Isolate the statistics we're interested in
        stats_list = []
        table = soup.find('table', id='rankings_table')

        # First row will be the column headings, the rest will be stats
        row_list = []
        sect_head = table.find('thead')
        cols_head = sect_head.findAll('th')
        for th in cols_head:
            row_list.append(th.text)
        stats_list.append(row_list)
        
        # For all other rows, pull out the team statistics / rankings
        sect_body = table.find('tbody')
        rows_body = sect_body.select('tr')
        for row in rows_body:
            row_list = []
            cols_body = row.select('td')
            for td in cols_body:
                row_list.append(td.text.strip())
            stats_list.append(row_list)
        full_stats_list.append(stats_list)
        time.sleep(5)

    #full_stats_list_flat = [y for x in full_stats_list for y in x]
    #return full_stats_list_flat

    return full_stats_list    
        
def player_stats(team):
    ''' This functions will scrape the NCAA College Football stats website
        and create a list that contains a team's player statistics for the
        specified year.
        
        Input:
            team (list) - a list containing all team information from a specific year.
                - team:    university name (string)
                - teamID:  ID number of the university (string)
                - yearID:  ID number of the year being queried (string)
                - year:    year being queried (string)
            
        Output:
            A list of lists where each list represents a statistical category.
            The 18 statistical categories are:
                Rushing, Passing, Receiving, Total Offense, All-Purpose Yards,
                Scoring, Sacks, Tackles, Passes Defended, Fumbles, Kicking,
                Punting, Punt Returns, Kickoffs and KO Returns, Redzone,
                Defense, Turnover Margin, Participation.
            Each category contains statistic specific to that area, but will
            contain the following data at a minimum:
                - jersey (string): player's jersey number
                - player (string): player's name in last name, first name format
                - pos (string):    player's position (1 or 2 letter code)
                - yr (string):     player's academic year (1 or 2 letter code)
                - gp (string):     player's number of games played in the season
                - gs (string):     player's number of games started in the season  
                - team (string):   team for which the player plays
                - year (string):   year of this player's information
    '''
    # Define a user agent so that they site doesn't forbid us from scraping data.
    headers = {"User-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"}
    
    # Specify the URL to be scraped based on the year that the user submits
    url = "http://stats.ncaa.org/team/" + team[1] + "/stats/" + team[2]
    
    # Make the request to the website and soupify the data
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.content)
    
    ### Determine the links for all the statistical categories
    table = soup.find('tr', class_ = 'heading')
    cols = table.findAll('td')
    
    category_dict = {'Rushing':''}
    for td in cols:
        if not isinstance(td.a, type(None)):
            category_dict[td.a.text] = int(str(td.a).split('=')[3].split('"')[0])
    category_dict['Rushing']=category_dict['Passing']-1
    
    ### Pull out statistics for each player
    full_stats_list = []
    for category in category_dict:
        print("Pulling category: " + category)
        url = "http://stats.ncaa.org/team/" + team[1] + "/stats?id=" + team[2] + "&year_stat_category_id=" + str(category_dict[category])
        r = requests.get(url, headers=headers)
        soup = BeautifulSoup(r.content)
        
        # Isolate the statistics we're interested in
        table = soup.find('table', id='stat_grid')
        rows = table.findAll('tr')
        
        # First row will be the column headings, the rest will be stats
        stats_list = []
        for i in range(0,len(rows)):
            row_list = []
            # If the first row, pull out the column headings (i.e. stat categories)
            if i == 0:
                cols = rows[i].findAll('th')
                for th in cols:
                    row_list.append(th.text)
            # If any other row, pull out the player statistics
            else:
                cols = rows[i].findAll('td')
                # Ignore summary rows (i.e. Totals for the Team and Opponents)
                ignored_rows = ['TEAM','Totals','Opponent Totals']
                if cols[1].text not in ignored_rows: 
                    for td in cols:
                        row_list.append(td.text)
            row_list.append(team[3])    # add in year for stats
            row_list.append(team[0])    # add in team for stats
            stats_list.append(row_list)
        full_stats_list.append(stats_list)
        time.sleep(5)
    
    full_stats_list_flat = [y for x in full_stats_list for y in x]

    return full_stats_list_flat


###############################################################################
# Working Code    

# Set the project working directory
os.chdir('/home/ejreidelbach/projects/CollegeFootball/')    

stats = team_stats(2018)

#### TEAM INFORMATION
    
# Create a list for the years we're interested in: 2013-14 to 2017-18
years_list = list(range(2014,2019))
teams_list = []

# Pull down the information from the website for those years
for year in years_list:
    teams_list.append(team_info(str(year)))

# A list of lists is returned; flatten it so we can add it to a dataframe
teams_list_flat = [y for x in teams_list for y in x]
teams_df = pd.DataFrame(teams_list_flat)

# Provide column headers and sort by team name then by year
teams_df.columns = ['team','teamID','yearID','year']
teams_df = teams_df.sort_values(['team','year']).reset_index(drop=True)
    
###############################################################################
#### ROSTER INFORMATION

# Pull down the information for a specific team (by referencing the teams_df)
roster_list = team_roster(list(teams_df.iloc[0,:]))
roster_df = pd.DataFrame(roster_list)
roster_df.columns = ['jersey','player','pos','yr','GP','GS','year','team']

###############################################################################
#### STATS INFORMATION

# Send a specific team's info to the function player_stats() and receive
# a list of lists containing all player stats for that year.


###############################################################################
#### RANKINGS INFORMATION

