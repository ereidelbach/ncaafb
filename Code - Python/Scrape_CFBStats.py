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
import os  
import pandas as pd
import pathlib
import requests
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
    r = requests.get(url)
    soup = BeautifulSoup(r.content,'html.parser')   
    return soup

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
    
    # Iterate over every year and place the year-URL combo into a dictionary
    for year in html_years_clean:
        dict_years[year.text] = year.find('a', href=True)['href']

    # Add the current year to the dictionary
    dict_years[team_url.split('/')[1].split('/')[0]] = team_url
    
    return dict_years

def directoryCheck(team_name):
    '''
    Purpose: Run a check of the /Data/CFBStats/ folder to see if a folder
        exists for the specified team.  If it doesn't, create it.
        
    Input:
        - team_name (string): Name of the school being scraped
    
    Outpu:
        - NONE
    '''
    pathlib.Path('Data/CFBStats/' + team_name).mkdir(parents=True, exist_ok=True)

def scrapeTeamRecords(team_name, dict_years):
    '''
    Purpose: Scrape the record's portion of a team's stats for all season
        
    Input:
        - team_name (string): Name of the school being scraped
        - dict_year (dictionary): Dictionary containing every year for which
            team statistics are recorded and the URL to those statistics
    
    Output:
        - A .csv file containing the scraped information (Team_Record.csv)
    '''
    # Create the master dictionary for storing records for all seasons/years
    dict_records = {}
    
    # Scrape each year contained in `dict_years`
    for year, url in dict_years.items():
        # Create a dictionary for the year being scraped
        dict_year = {}
        
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
                dict_year[split_name + ' (Win %)'] = int(
                    dict_year[split_name + ' (Wins)']) / int(
                            dict_year[split_name + ' (Losses)'])
                
            # Add year to master dictionary
            dict_records[year] = dict_year
    
    # Convert `dict_records` to a Pandas DataFrame
    df_records = pd.DataFrame.from_dict(dict_records, orient='index')
    
    # Check to see if the output directory exists (and if not -> make it)
    directoryCheck(team_name)
    
    # Export the newly created Pandas DataFrame to a .csv
    df_records.to_csv('Data/CFBStats/' + team_name + '/records.csv')
    
def scrapeTeamSchedules(dict_years):
    '''
    Purpose: Scrape the schedule portion of a team's stats for all seasons
        
    Input:
        - dict_year (dictionary): Dictionary containing every year for which
            team statistics are recorded and the URL to those statistics
    
    Output:
        - A .csv file containing the scraped information (Team_Schedule.csv)
    '''

def scrapeTeamStats(team_name, team_url):
    '''
    Purpose: Scrape all the statistical information for a college team
        on CFBStats.com and export it to a series of .csv files
        
    Input:
        - team_name (string): Name of the school being scraped
        - team_url (string): URL of the school's page on CFBStats.com
    
    Output:
        - xxx
    '''
    dict_years = scrapeTeamYears(team_url)
    
'''
- soup (HTML): BeautifulSoup version of HTML for a team's season    
'''
    
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
    scrapeTeamStats(team_name, team_url)