#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 16:21:10 2019

@author: ejreidelbach

:DESCRIPTION: Scrapes player-specific information for college players from
            sports-reference.com

:REQUIRES:
    Refer to the Package Import section of the script
    
:TODO:
    TBD
"""
 
#==============================================================================
# Package Import
#==============================================================================
import csv
import glob
import numpy as np
import os  
import pandas as pd
import pathlib
import requests
import tqdm

from bs4 import BeautifulSoup
#from standardize_names_and_logos import rename_teams
from code_python.standardize_names_and_logos import rename_teams
from urllib3.util import Retry
from string import digits

#==============================================================================
# Reference Variable Declaration
dict_positions = {'QB' : 'QB',
                  'OLB'  : 'LB', 
                  'ILB'  : 'LB', 
                  'LB'   : 'LB', 
                  'LILB' : 'LB', 
                  'RILB' : 'LB', 
                  'MLB'  : 'LB', 
                  'LLB'  : 'LB', 
                  'RLB'  : 'LB', 
                  'SLB'  : 'LB', 
                  'SAM'  : 'LB', 
                  'LOLB' : 'LB', 
                  'ROLB' : 'LB',
                  'WILL' : 'LB',
                  'WLB'  : 'LB',
                  'MIKE' : 'LB',
                  'RUSH' : 'LB',
                  'BLB' : 'LB',
                  'NCB' : 'DB',
                  'DB'  : 'DB', 
                  'CB'  : 'DB', 
                  'RCB' : 'DB', 
                  'LCB' : 'DB', 
                  'S'   : 'DB', 
                  'SS'  : 'DB', 
                  'FS'  : 'DB',
                  'LDH' : 'DB',
                  'RDH' : 'DB',
                  'RS'  : 'DB',
                  'D'   : 'DL',
                  'EDGE': 'DL',
                  'D/E' : 'DL',
                  'DE'  : 'DL', 
                  'DL'  : 'DL', 
                  'LDE' : 'DL', 
                  'RDE' : 'DL', 
                  'DT'  : 'DL', 
                  'RDT' : 'DL', 
                  'LDT' : 'DL', 
                  'NT'  : 'DL',
                  'MG'  : 'DL',
                  'DG'  : 'DL',
                  'WDE' : 'DL',
                  'NG'  : 'DL',
                  'SDE' : 'DL',
                  'RB' : 'RB',
                  'SB' : 'RB',
                  'HB' : 'RB',
                  'TB' : 'RB',
                  'FB' : 'RB',
                  'LH' : 'RB',
                  'RH' : 'RB',
                  'BB' : 'RB',
                  'B'  : 'RB',
                  'WB' : 'RB',
                  'FL' : 'WR',
                  'WR' : 'WR',
                  'SE' : 'WR',
                  'E'  : 'TE',
                  'TE' : 'TE',
                  'LE' : 'OL',
                  'LOT': 'OL',
                  'LT' : 'OL',
                  'RT' : 'OL',
                  'ROT': 'OL',
                  'RE' : 'OL',
                  'LG' : 'OL',
                  'RG' : 'OL',
                  'G'  : 'OL',
                  'OL' : 'OL',
                  'C'  : 'OL',
                  'T'  : 'OL',
                  'OT' : 'OL',
                  'OG' : 'OL',
                  'LS' : 'OL',
                  'PK': 'K',
                  'K' : 'K',
                  'P' : 'P'
        }

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

def process_player_info(soup, player_id):
    '''
    Purpose: Scrapes player metadata from sports-reference.com

    Inputs
    ------
        soup : BeautifulSoup Data
            A BeautifulSoup representation of a player's profile page
        player_id : string
            The player's unique identifier on sports-reference.com
    
    Outputs
    -------
        dict_player : dictionary
            A dictionary containing a player's metadata
    '''
    player = soup.find('div', {'itemtype':'https://schema.org/Person'})

    # Initalize all storage variables
    list_schools, list_position, height, weight = ('', [], '', '')
    draft_round, draft_pick, draft_team, draft_year = ('', '', '', '')
    nameFirst, nameLast, nameFull = ('', '', '')
            
    #--------- NAME -------------
    nameFull = player.find('h1', {'itemprop':'name'}).text
    # if no name exists, skip the player entirely
    if nameFull == '':
        dict_player = {}
        dict_player['id_ncaa'] = player_id
        for key in ['player', 'school', 'pos_ncaa', 'pos_ncaa_std', 'name_first', 
                    'name_last', 'id_ncaa', 'from_ncaa', 'to_ncaa', 'height', 
                    'weight', 'draft_round', 'draft_pick', 'draft_team', 
                    'draft_year', 'list_schools']:
            dict_player[key] = ''
        return dict_player 
    
    nameFirst = nameFull.split(' ')[0]
    try:
        nameLast = nameFull.split(' ')[1]
    except:
        nameLast = ''
        
    # remove any non-alphabetic characters from the names
    nameFull = ''.join(char for char in nameFull if (char.isalnum() or char == ' '))
    nameFirst = ''.join(char for char in nameFirst if (char.isalnum() or char == ' '))
    nameLast = ''.join(char for char in nameLast if (char.isalnum() or char == ' '))
        
    #--------- SCHOOL -----------
    try:
        list_schools = []
        list_links = list(player.findAll('a'))
        for index, link in enumerate(list_links):
            if 'school' in link['href']:
                list_schools.append(link)
        list_schools = [x.text for x in list_schools]
        
        # only take the most current school for a player
        school = list_schools[-1]
    except:
        school = ''
    
    #--------- POSITION -----------        
    list_info = player.findAll('p')
    for info in list_info:
        if 'Position' in str(info):
            position = str(info).split(': ')[1].split('\n')[0]
            # rename HB to RB
            position = position.replace('HB','RB')
            # add position info to a list
            list_position = position.split('/')
        #--------- HEIGHT -----------
        elif 'height' in str(info):
            height = str(info).split('height">')[1].split('</span>')[0] 
            #--------- WEIGHT -----------
            try:
                weight = int(str(info).split('weight">')[1].split('lb')[0])
            except:
                weight = ''
        #--------- DRAFT -----------
        elif 'NFL draft' in str(info):
            draftInfo = str(info).split(
                    'Draft:</strong> ')[1].split(' overall')[0]
            draft_round = int(draftInfo[0])
            draft_pick = int(''.join(c for c in draftInfo.split(
                    'round, ')[1] if c in digits))
            draft_team = info.findAll('a')[-1].text
            draft_year = int(str(info).split('years/')[1].split('/draft.htm')[0])
                       
    #--------- ELIGIBILITY from Stats Table -----------
    # extract column headers from the first stat category the player has
    try:
        headers = soup.find('div', {'class':'table_outer_container'}).find(
                'table').find('thead').find_all('tr')[-1].find_all('th')
        # convert the column headers to a list of strings
        list_headers = [x.text for x in headers]
        # extract the data for the player in the first stat category they have
        df_body = pd.read_html(str(soup.find('div', {
                'class':'table_outer_container'}).find('table')))[0]
        # insert the column headers
        df_body.columns = list_headers
        # remove any rows subsequent to (and including) "Career"
        df_body = df_body[:df_body[df_body['Year'] == 'Career'].index[0]]
        # determine the first season the player played football
        year_from = int(''.join(
                c for c in df_body.iloc[0]['Year'] if c in digits))
        # extract the last year of football they plyed
        year_last = int(''.join(
                c for c in df_body.iloc[-1]['Year'] if c in digits)) 
        # check to see if they have eligiblity remaining
        if year_last == 2019:
            # determine their grade for the last year
            # check the last row of the player table for their class
            grade = df_body.iloc[-1]['Class']
            # if a value doesn't exist, check to see if exists in other rows
            if pd.isna(grade):
                if len(df_body) == 1:   # if only one row exists, label them freshman
                    grade = 'FR'
                elif len(df_body) > 1:
                    row_count = 1
                    while (pd.isna(grade) and row_count <= len(df_body)):
                        grade = df_body.iloc[-row_count]['Class']
                        row_count = row_count + 1
            if grade == 'SR':
                pass
            elif grade == 'JR':
                year_last = year_last + 1   # offset extra year to account for draft
            elif grade == 'SO':
                year_last = year_last + 2   # offset extra year to account for draft
            elif grade == 'FR':
                year_last = year_last + 3   # offset extra year to account for draft
            else:
                print('Error detected in grade (%s) for player: %s' % (
                        grade, nameFull))
        # offset by extra year to account for draft
        year_last = year_last + 1
    except:
        year_from = ''
        year_last = ''
            
    #--------- POSITION from Stats Table -----------       
    # iterate over every year in the table and add the player's position to the
    #   position list if it doesn't already exist
    try:
        for index, row in df_body.iterrows():
            pos_year = row['Pos']
            if pos_year not in list_position:
                if not pd.isna(pos_year):
                    list_position.append(pos_year)
    except:
        pass
     
    #--------- Standardize Position Variable -----------
    # create a standardized version of the position variable (i.e. CB or S = DB)
    list_position_std = []
    for pos in list_position:
        if ((pos in dict_positions.keys()) and 
            (dict_positions[pos] not in list_position_std)):
            list_position_std.append(dict_positions[pos])
                
    #---------- DICTIONARY ------------------
    # put player info into a dictionary format
    dict_player = {}
    dict_player['id_ncaa'] = player_id
    dict_player['school'] = school
    dict_player['pos_ncaa'] = list_position
    dict_player['pos_ncaa_std'] = list_position_std
    dict_player['height'] = height
    dict_player['weight'] = weight
    dict_player['draft_round'] = draft_round
    dict_player['draft_pick'] = draft_pick
    dict_player['draft_team'] = draft_team
    dict_player['draft_year'] = draft_year
    dict_player['name_first'] = nameFirst
    dict_player['name_last'] = nameLast
    dict_player['player'] = nameFull
    dict_player['from_ncaa'] = year_from
    dict_player['to_ncaa'] = year_last
    dict_player['list_schools'] = process_player_teams(soup)
    
    return dict_player

def process_player_teams(soup):
    '''
    Purpose: Scrapes player metadata from sports-reference.com

    Inputs
    ------
        soup : BeautifulSoup Data
            A BeautifulSoup representation of a player's profile page
    
    Outputs
    -------
        list_teams : list of tuples
            Contains a series of year-team key-value pairs for each year in a 
            player's career (i.e. year the player played and the team they 
            played for)
    '''    
    # scrape the player's data
    try:
        headers = soup.find('div', {'class':'table_outer_container'}).find(
                'table').find('thead').find_all('tr')[-1].find_all('th')
        # convert the column headers to a list of strings
        list_headers = [x.text for x in headers]       
        # extract the player's statistical data (in table format)
        df_body = pd.read_html(str(soup.find('div', {
                'class':'table_outer_container'}).find('table')))[0]
        # insert the column headers
        df_body.columns = list_headers
    except:
        return []

    # remove any rows subsequent to (and including) "Career"
    if isinstance(df_body.columns, pd.MultiIndex):
        df_body.columns = df_body.columns.droplevel()
    df_body = df_body[:df_body[df_body['Year'] == 'Career'].index[0]]

    # remove any non integer characters from year values (i.e. pro bowl indicators)
    df_body['Year'] = df_body['Year'].apply(lambda x: int(''.join(
            [char for char in x if char in digits])))

    # standardize the name of the player's school
    dict_teams = {}
    for index, row in df_body.iterrows():
        team = rename_teams([row['School']], 'ncaa', 'Team')[0]
        dict_teams[row['Year']] = team
        
    # isolate the teams the player played for each year
    df_teams = pd.DataFrame.from_dict(dict_teams, 
                                      orient = 'index', 
                                      columns = ['team'])
    # iterate over each year and fill in missing years for a player with their
    #   previous team (i.e. if a player plays in 2012 and 2014, assign their
    #   2013 season to their 2012 team)
    for year in range(df_teams.iloc[0].name, df_teams.iloc[-1].name + 1):
        if year not in dict_teams.keys():
            dict_teams[year] = dict_teams[year - 1]

    # convert dictionary to list of tuples for easier writing-to-disk
    list_teams = sorted(
            [(year, school) for year, school in dict_teams.items()], key = (
                    lambda x: x[0]))

    return list_teams

def merge_current_year_with_all(year):
    '''
    Purpose: Merges newly scraped data from the most recent year with the 
        metadata table containing players from all scraped years.

    Inputs
    ------
        year : int
            Year of data which is to be scraped (only required if 'year' is 
            specified in `scrape_type`)
    
    Outputs
    -------
        player_lookup_ncaa.csv : CSV File
            File written to disk that contains metadata on ALL scraped players
    '''
    # read in new data
    df_year = pd.read_csv(
            'data_scraped/ncaa_player_meta/player_lookup_ncaa_%s.csv' % str(year))
    # read in pre-existing metadata
    df_all = pd.read_csv('data_scraped/ncaa_player_meta/player_lookup_ncaa.csv')

    # backup old file before overwriting
    df_all.to_csv('data_scraped/ncaa_player_meta/player_lookup_ncaa_backup.csv', 
                  index = False)

#    # write a list of the new players to disk for review
#    list_new = [x for x in df_year['id_ncaa'] if x not in df_all['id_ncaa']]
#    df_new = df_year[df_year['id_ncaa'].isin(list_new)]
#    if len(df_new) > 0: 
#        print('%i new players found in NCAA data' % len(df_new))
#        df_new.to_csv('data_scraped/ncaa_player_meta/new_players.csv', 
#                      index = False)
    
    # merge new and pre-existing data on 'id_ncaa'
    df_merged = pd.merge(df_all,
                         df_year,
                         how = 'outer',
                         left_on = 'id_ncaa',
                         right_on = 'id_ncaa',
                         suffixes = ('_all', '_year'))
    
    #---------- Handle Duplicate Columns -------------------------------------#
    # fill in variables to account for overlap across the two files
    for variable in df_year.columns.tolist():
        # skip ncaa ID
        if variable == 'id_ncaa':
            pass
        # keep the _YEAR version unless it is missing, then go with _ALL version
        else:
            df_merged[variable] = df_merged.apply(lambda row:
                row['%s_all' % variable] if (pd.isna(row['%s_year' % variable]) 
                or row['%s_year' % variable] == '') else 
                row['%s_year' % variable], axis = 1) 
        
    # drop _ALL and _YEAR variables once merged variable is created
    for variable in df_year.columns.tolist():
        # skip ncaa ID
        if variable == 'id_ncaa':
            pass
        else:
            df_merged = df_merged.drop(
                    ['%s_all' % variable, '%s_year' % variable], axis = 1)    
            
    # drop duplicates
    df_merged = df_merged.drop_duplicates()
    
    # check if we have any players with more than 2 entries
    counts = df_merged['id_ncaa'].value_counts()
    count_list = counts[counts >= 2].index.tolist()
    df_dups = df_merged[df_merged['id_ncaa'].isin(count_list)]
    list_dups = [x for x in df_dups['id_ncaa'].drop_duplicates()]
    for dup in list_dups:
        print('WARNING:  Duplicate entry found for player: %s in NCAA metadata' 
              % dup)
            
    # reorder columns for data
    df_merged = df_merged[[
            'player', 'school', 'pos_ncaa', 'pos_ncaa_std', 
            'name_first', 'name_last', 'id_ncaa', 'from_ncaa', 'to_ncaa', 
            'height', 'weight', 'draft_round', 'draft_pick', 'draft_team', 
            'draft_year', 'url_pic_player', 'list_schools']]
    
    # fill NANs with empty strings
    df_merged = df_merged.fillna('')
    
    # write merged file to disk
    df_merged.to_csv('data_scraped/ncaa_player_meta/player_lookup_ncaa.csv', 
                     index = False)
    
    return

def scrape_players(scrape_type, year = 0):
    '''
    Purpose: Scrapes player metadata from sports-reference.com for all 
        collegiate players or only those in the specified year (per variable `year`)

    Inputs
    ------
    scrape_type : string
        'all' --> scrape data for all players for whom elo data exists
        'year' --> scrape data for only those players found in a specified
            year's elo data
    year : int
        Year of data which is to be scraped (only required if 'year' is 
        specified in `scrape_type`)
    
    Outputs
    -------
        player_lookup_ncaa_xxxx.csv : CSV File
            File written to disk that contains metadata on scraped players
    '''    
    # set the file name for active players and ingest player IDs for scraping
    if scrape_type == 'all':
        filename = 'data_scraped/ncaa_player_meta/player_lookup_ncaa_all.csv'
        list_player_ids = []
        for file in sorted(glob.glob('data_scraped/ncaa_player/*.csv')):
            list_ids_year = list(pd.read_csv(file)['unique_id'])
            # remove nans
            list_ids_year = [x for x in list_ids_year if str(x) != 'nan']
            # add players to cumulative list and dedup
            list_player_ids = sorted(list(set(list_player_ids + list_ids_year)))
    elif scrape_type == 'year':
        filename = ('data_scraped/ncaa_player_meta/player_lookup_ncaa_%s.csv' 
                    % str(year))
        # sort and dedup list
        list_player_ids = sorted(list(set(pd.read_csv(
            'data_scraped/ncaa_player/ncaa_player_%s.csv' % str(year))['unique_id'])))   
    else:
        print('Incorrect value for scrape-type, please try again.')
        return
    
    # set variable names to be used in output CSV file
    headers = ['player', 'school', 'pos_ncaa', 'pos_ncaa_std', 'name_first', 
               'name_last', 'id_ncaa', 'from_ncaa', 'to_ncaa', 'height', 
               'weight', 'draft_round', 'draft_pick', 'draft_team', 'draft_year',
               'list_schools']

    #Open the CSV writer
    with open(filename, 'w') as f:
        # Initialize the CSV file writer
        wr = csv.writer(f)
        
        # Write the headers of the CSV
        wr.writerow(headers)

        # for every player, query SportsReference and get their metadata
        for player_id in tqdm.tqdm(list_player_ids):
            # check for missing player_ids
            if pd.isna(player_id):
                continue
            
            # scrape a player's profile page
            url = ('https://www.sports-reference.com/cfb/players/' + 
                   player_id + '.html')
            soup = soupify_url(url)
            
            # Ensure data has been found and no '404 error' exists:
            if 'Page Not Found (404 error)' in str(soup):
                dict_player = {}
                dict_player['unique_id'] = player_id
            else:
                dict_player = process_player_info(soup, player_id)  
             
            try:
                writeout = [dict_player[head] for head in headers]
            except:
                print('Error writing data for player: %s' % player_id)
                
            # write out the rows of data
            wr.writerow(writeout)     
    
    # if data was only scraped for a specific year, merge that data with the
    #   historic table containing all players
    if scrape_type == 'year':
        merge_current_year_with_all(year)
    
    return
    
def main():
    pass

#=============================================================================
# Working Code
#==============================================================================
if __name__ == "__main__":
    main()
    
# Set the project working directory
#path_dir = pathlib.Path('/home/ejreidelbach/Projects/draft-analytics-data')
#os.chdir(path_dir)
#
#scrape_players('year', 2019)
#scrape_players('all')