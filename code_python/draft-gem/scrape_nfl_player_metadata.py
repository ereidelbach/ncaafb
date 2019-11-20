#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 27 09:57:15 2019

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
import datetime
import glob
import numpy as np
import os  
import pandas as pd
import pathlib
import requests
import tqdm

from bs4 import BeautifulSoup
from code_python.standardize_names_and_logos import rename_teams
from urllib3.util import Retry
from string import digits

#==============================================================================
# Reference Variable Declaration
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
    Purpose: Scrapes player metadata from pro-football-reference.com

    Inputs
    ------
        soup : BeautifulSoup Data
            A BeautifulSoup representation of a player's profile page
        player_id : string
            The player's unique identifier on pro-football-reference.com
    
    Outputs
    -------
        dict_player : dictionary
            A dictionary containing a player's metadata
    '''
    # Find the player's data in the BeautifulSoup format and extract the info
    info = soup.find('div', {'id':'meta'})

    # Initialize player dictionary for storing all scraped metadata
    dict_player = {}
    dict_player['id_nfl'] = player_id

    #--------- PICTURE -------------#
    try:
        pictureURL = info.find('img')['src']
    except:
        pictureURL = ''
    
    dict_player['url_pic_player'] = pictureURL
        
    #--------- NAME (Full, First and Last) -------------#
    try:
        nameFull = info.find('h1', {'itemprop':'name'}).text
    except:
        nameFull = ''
    try:
        nameFirst = nameFull.split(' ')[0]
    except:
        nameFirst = ''
    try:
        nameLast = nameFull.split(' ')[1]
    except: 
        nameLast = ''
    
    dict_player['name_first'] = nameFirst
    dict_player['name_last'] = nameLast
    dict_player['player'] = nameFull
    
    #--------- POSITION -------------#
    try:
        position = info.find(
            'strong', text='Position').next_sibling.strip().split(': ')[1]
        position = position.split('-')
    except:
        position = ['']
    
    dict_player['pos_nfl'] = position
    
    # standardize position values
    position_std = []
    # for every position a player has, swap out the value for a std version
    for pos in position:
        if pos in dict_positions.keys():
            # extract the standardized value
            pos_std = dict_positions[pos]
            # add the standardized position if it doesn't already exist
            if pos_std not in position_std:
                position_std.append(pos_std)
        else:
            pass
    dict_player['pos_nfl_std'] = position_std
    
    #--------- HEIGHT and WEIGHT -------------#
    try:
        height = info.find('span', {'itemprop':'height'}).text
#        # convert height to inches
#        height = int(height.split("-")[0])*12 + int(height.split("-")[1])
    except:
        height = ''
    try:
        weight = info.find('span', {'itemprop':'weight'}).text
        weight = int(''.join(c for c in weight if c in digits))
    except:
        weight = ''
        
    dict_player['height'] = height
    dict_player['weight'] = weight
    
    #--------- TEAM -------------#
    try:
        team = info.find('span', {'itemprop':'affiliation'}).text
    except:
        team = ''
    
    dict_player['team_nfl'] = team
    
    #--------- AGE -------------#
    try:
        # extract birthday
        age = info.find('span', {'itemprop':'birthDate'})['data-birth']
        # calculate age in years from today's date
        age = int((datetime.datetime.today() - 
                   datetime.datetime.strptime(age, '%Y-%m-%d')).days/365)
    except:
        age = ''
    
    dict_player['age_nfl'] = age

    #--------- SCHOOL -------------#
    # players can have more than one school, so we isolate the last value
    try:
        player = soup.find('div', {'itemtype':'https://schema.org/Person'})
        list_schools = []
        list_paragraphs = list(player.findAll('p'))
        for index, para in enumerate([str(x) for x in list_paragraphs]):
            if 'College' in para:
                list_links = list_paragraphs[index].find_all('a')
                for link in list_links:
                    try:
                        # ignore high schools
                        if (('schools' in link['href']) and 
                            ('high_schools' not in link['href'])):
                            list_schools.append(link.text)
                    except:
                        pass
 
       # only take the most current school for a player
        school = list_schools[-1]   
    except:
        school = ''
        
    dict_player['school'] = school
    
    #--------- Sports-Reference URL/ID -------------# 
    try:
        collegeURL = info.find('a', text='College Stats')['href']
        collegeID = collegeURL.split('/')[-1].replace('.html','')
    except:
        collegeURL = ''
        collegeID = ''

    dict_player['id_ncaa'] = collegeID
    
    #--------- DRAFT -----------
    try:
        pick_info = info.find('strong', 
                              text='Draft').next_sibling.next_sibling.next_sibling
        # round drafted
        draft_round = int(''.join(c for c in pick_info.split(
                ' round')[0] if c in digits))
        # pick selected
        draft_pick = int(''.join(c for c in pick_info.split(
                ' round')[1] if c in digits))
        # team that drafted player
        draft_team = info.find('strong', 
                               text='Draft').next_sibling.next_sibling.text
        # year draft took place
        draft_year = int(''.join(
                c for c in pick_info.next_sibling.text if c in digits))
    except:
        draft_round = ''
        draft_pick = ''
        draft_team = ''
        draft_year = ''
        
    dict_player['draft_round'] = draft_round
    dict_player['draft_pick'] = draft_pick
    dict_player['draft_team'] = draft_team
    dict_player['draft_year'] = draft_year        
    dict_player['list_teams'] = process_player_teams(soup)
        
#    #--------- BIRTHDAY  -------------#
#    try:
#        birthday = info.find('span', {'itemprop':'birthDate'})['data-birth']
#    except:
#        birthday = '0000-00-00'
        
#    #--------- HOMETOWN -------------#
#    try:
#        birthplace = ' '.join(info.find('span', {
#            'itemprop':'birthPlace'}).text.strip().split('\xa0')[1:])
#    except:
#        birthplace = ''

    # extract additional stats (Career AV, yearly AV, and a team if necessary)
    dict_player = extract_additional_stats(soup, dict_player)
    
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
    try:
        df_body = pd.read_html(str(soup.find('div', {
                'id':'content'}).find_all('table')[-1]))[0]
    except:
        df_body = pd.DataFrame()   

    # remove any rows subsequent to (and including) "Career"
    if isinstance(df_body.columns, pd.MultiIndex):
        df_body.columns = df_body.columns.droplevel()
    try:
        df_body = df_body[:df_body[df_body['Year'] == 'Career'].index[0]]
    except:
        if 'Career' not in df_body['Year']:
            pass

    # if a player misses a year due to injury, default to the previous team
    list_teams = []
    prev_team = ''
    for index, row in df_body.iterrows():
        if len(row['Tm']) > 3:
            list_teams.append(prev_team)
        else:
            prev_team = row['Tm']
            list_teams.append(row['Tm'])
    df_body['Tm'] = list_teams
    
    # correct issues where a player plays for multiple teams in one year
    #   only keep the total and make the last team played for the year's team
    list_rows = []
    for index, row in df_body.iterrows():
        if row['Tm'] == '2TM':
            row['Tm'] = df_body.iloc[index+2]['Tm']
            row['No.'] = df_body.iloc[index+2]['No.']
        elif row['Tm'] == '3TM':
            row['Tm'] = df_body.iloc[index+3]['Tm']
            row['No.'] = df_body.iloc[index+3]['No.']
        elif row['Tm'] == '4TM':
            row['Tm'] = df_body.iloc[index+4]['Tm']
            row['No.'] = df_body.iloc[index+4]['No.']
        if (not pd.isna(row['Year'])) and (len(str(row['Year'])) > 3):
            list_rows.append(row.loc['Year':'Tm'])
    df_body = pd.DataFrame(list_rows)

    # remove any non integer characters from year values (i.e. pro bowl indicators)
    df_body['Year'] = df_body['Year'].apply(lambda x: int(''.join(
            [char for char in str(x) if char in digits])))

    # standardize the name of the player's school
    dict_teams = {}
    for index, row in df_body.iterrows():
        team = rename_teams([row['Tm']], 'nfl', 'Team')[0]
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
            [(year, team) for year, team in dict_teams.items()], key = (
                    lambda x: x[0]))

    return list_teams

def extract_additional_stats(soup, dict_player):
    '''
    Purpose: Extract additional meatadata from pro-football-reference.com 
        - Career AV, yearly AV, and a team if one isn't ready stored

    Inputs
    ------
        soup : BeautifulSoup Data
            A BeautifulSoup representation of a player's profile page
        dict_player : dictionary
            contains all scraped metadata for the specified player
    
    Outputs
    -------
        list_players : list of dictionaries
            List of dictionaries which contain metadata about each player
    '''  
    #--------- Isolate Statistics based on position -------------#    
    try:
        df_stats = pd.read_html(str(soup.find('div', {'id':'content'}).find_all(
                'table')[-1]))[0]
    except:
        df_stats = pd.DataFrame()        
        
    # if no data found for player, exit
    if len(df_stats) == 0:
        print("No additional data found for %s" % (dict_player['player']))
        dict_player['from_nfl'] = ''
        dict_player['to_nfl'] = ''
        dict_player['av_career'] = ''
        dict_player['av_historic'] = ''
        return(dict_player)            
    
    #--------- Conduct Dataframe Cleanup -------------------------------------#
    # Clean up column names if multi-tier indexing exists
    df_stats.columns = extract_column_names(df_stats.columns.tolist())
    
    # correct issues where a player plays for multiple teams in one year
    #   only keep the total and make the last team played for the year's team
    list_df_rows = []
    for index, row in df_stats.iterrows():
        if row['Tm'] == '2TM':
            row['Tm'] = df_stats.iloc[index+2]['Tm']
            row['No.'] = df_stats.iloc[index+2]['No.']
        elif row['Tm'] == '3TM':
            row['Tm'] = df_stats.iloc[index+3]['Tm']
            row['No.'] = df_stats.iloc[index+3]['No.']
        elif row['Tm'] == '4TM':
            row['Tm'] = df_stats.iloc[index+4]['Tm']
            row['No.'] = df_stats.iloc[index+4]['No.']
            
        if type(row['Year']) == str:
            try:
                row['Year'] = int(str(row['Year']).replace('*','').replace('+',''))
            except:
                pass
        list_df_rows.append(row)
    df_stats = pd.DataFrame(list_df_rows)
    
    # drop the rows for which the year value is empty (i.e. multi-team years)
    df_stats = df_stats.dropna(subset = ['Year'])
    
    # account for years in which a player missed the season due to injury
    for column in df_stats.columns.tolist():
        df_stats[column] = df_stats[column].apply(
                lambda x: '' if 'Missed season' in str(x) else x)
    
    # determine what row contains 'Career' info
    try:
        career_index = df_stats['Year'].tolist().index('Career')
    except:
        # some players don't have a 'Career' row summarizing stats
        try:
            for row in df_stats['Year'].tolist():
                if 'yrs' in row:
                    career_index = df_stats['Year'].tolist().index(row)
                    break
        except:
            career_index = len(df_stats)

    #--------- TEAM ----------------------------------------------------------#    
    # fill in team info if one does not exist
    if dict_player['team_nfl'] == '':
        row_index = career_index
        team = 'nan'
        while str(team) == 'nan':
            row_index = row_index - 1
            team = df_stats.iloc[row_index]['Tm']
            
        dict_player['team_nfl'] = team    
    else:
        pass
    
    #--------- AV ------------------------------------------------------------#    
    # check if AV is missing from the table (i.e. kick/punt returners)
    if 'AV' not in df_stats:
        try:
            df_returns = pd.read_html(str(soup.find('table', {'id':'returns'})))[0]
            if 'AV' in df_returns.columns:
                df_stats['AV'] = df_returns['AV']
        except:
            pass
    
    # career AV
    try:
        av_career = df_stats.iloc[career_index]['AV']
        dict_player['av_career'] = av_career
    except:
        try:
            av_career = df_stats['AV'].sum()
            dict_player['av_career'] = av_career
        except:
            dict_player['av_career'] = ''

    # historic AV (i.e. AV for each season)
    try:
        av_historic = df_stats['AV'].tolist()[0:career_index]
        av_historic = [int(str(x).replace("'","")) if ((x != '') or (
                x != 'nan')) else np.nan for x in av_historic]
        dict_player['av_historic'] = av_historic
    except:
        dict_player['av_historic'] = ''

    #--------- FROM (i.e. first year played) ---------------------------------#    
    try:
        year_from = df_stats.iloc[0]['Year']
        dict_player['from_nfl'] = int(''.join(c for c in str(year_from) if c in digits))
    except:
        dict_player['from_nfl'] = ''

    #--------- TO (i.e. last year played) ------------------------------------#    
    try:
        year_last = df_stats.iloc[career_index - 1]['Year']
        # account for players who play for multiple teams in their last season
        if (pd.isna(year_last)) or (year_last == '*'):
            row_index = career_index - 1
            while (pd.isna(year_last)) or (year_last == '*'):
                row_index = row_index - 1
                year_last = df_stats.iloc[row_index]['Year']
        dict_player['to_nfl'] = int(''.join(c for c in str(year_last) if c in digits))
    except:
        dict_player['to_nfl'] = ''
    
    #--------- POSITION ------------------------------------------------------#
    # It's possible for the position in the player header to differe from 
    #   the positions stored in the player's statistical table rows.  We'll 
    #   extract the positions in the tables and add it to any pre-existing data

    # retrieve position data that was previously scraped
    list_positions = dict_player['pos_nfl']
    
    # extract position data from table and insert into the list if not present
    positions_new = df_stats.iloc[0:career_index]['Pos'].tolist()
    for position in positions_new:  
        # handle positions which are listed like CB/LCB
        for sub_position in str(position).split('/'):
            # ensure the string is capitalized for matching purposes
            sub_position = sub_position.upper()
            # if not in the original list, add the position
            if (sub_position not in list_positions) and (sub_position != 'NAN'):
                list_positions.append(sub_position)    
    
    dict_player['pos_nfl'] = list_positions
    
    # standardize position values
    list_positions_std = []
    # for every position a player has, swap out the value for a std version
    for pos in list_positions:
        if pos in dict_positions.keys():
            # extract the standardized version of the position
            pos_std = dict_positions[pos]
            # if the position is not in the standardized list, add it
            if pos_std not in list_positions_std:
                list_positions_std.append(pos_std)
        else:
            pass
    dict_player['pos_nfl_std'] = list_positions_std
    
    return dict_player
 
def extract_column_names(list_columns):
    '''
    Purpose: Given a dataframe with multi-tiered column names, determine
        the correct column name and return it for proper formatting purposes

    Inputs
    ------
        list_columns : list of strings
            Unformatted column names
    
    Outputs
    -------
        list_columns_new : list of strings
            New column names that are properly formatted
    '''  
    list_columns_new = []
    
    # handle passing statistics
    if 'QBrec' in list_columns:
        for column in list_columns:
            if column not in ['Year','Age','Tm','Pos','No.','G','GS','QBrec', 'AV']:
                list_columns_new.append('Pass_' + column)
            else:
                list_columns_new.append(column)
    # handle Kicking and Punting
    elif '0-19' in str(list_columns):
        list_columns_new = ['Year', 'Age', 'Tm', 'Pos', 'No.', 'G', 'GS',
                        'FG_0_to_19_Att', 'FG_0_to_19_Made',
                        'FG_20_to_29_Att', 'FG_20_to_29_Made',
                        'FG_30_to_39_Att', 'FG_30_to_39_Made',
                        'FG_40_to_49_Att', 'FG_40_to_49_Made',
                        'FG_50+_Att', 'FG_50+_Made', 'FGA', 'FGM', 'Long',
                        'FG%', 'XPA', 'XPM', 'XP%', 'Punts', 'Punt_Yds',
                        'Punt_Long', 'Punt_Blck', 'Punt_Y/P', 'AV'
                        ]
    # handle rushing, receiving or defensive statistics (multi-tiered names)
    else:  
        for pair in list_columns:
            if any(x in pair[0] for x in ['Unnamed', 'Games']):
                list_columns_new.append(pair[1])
            elif 'Rushing' in pair[0]:
                list_columns_new.append('Rush_' + pair[1])
            elif 'Receiving' in pair[0]:
                list_columns_new.append('Rec_' + pair[1])
            elif 'Total Yds' in pair[0]:
                list_columns_new.append('Tot_' + pair[1])         
            elif 'Def Interceptions' in pair[0]:
                list_columns_new.append('INT_' + pair[1])
            elif 'Fumbles' in pair[0]:
                list_columns_new.append('Fum_' + pair[1])
            elif 'Tackles' in pair[0]:
                list_columns_new.append('Tkl_' + pair[1])    
            elif 'Punt Returns' in pair[0]:
                list_columns_new.append('Punt_Ret_' + pair[1])
            elif 'Kick Returns' in pair[0]:
                list_columns_new.append('Kick_Ret_' + pair[1])
            elif type(pair) == str:
                list_columns_new.append(pair)
                
    return list_columns_new    

def merge_current_year_with_all(year = ''):
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
        player_lookup_nfl.csv : CSV File
            File written to disk that contains metadata on ALL scraped players
    '''
    # read in metadata for newest year
    df_year = pd.read_csv(
            'data_scraped/nfl_player_meta/player_lookup_nfl_%s.csv' % str(year))
    # read in pre-existing data
    df_all = pd.read_csv('data_scraped/nfl_player_meta/player_lookup_nfl.csv')
    
    # backup old file
    df_all.to_csv('data_scraped/nfl_player_meta/player_lookup_nfl_backup.csv', 
                  index = False)    
 
    # write a list of the new players to disk for review
    list_new = [x for x in df_year['id_nfl'] if x not in df_all['id_nfl']]
    df_new = df_year[df_year['id_nfl'].isin(list_new)]
    if len(df_new) > 0: 
        print('%i new players found in NFL data' % len(df_new))
        
    # merged new and pre-existing data on 'id_nfl'
    df_merged = pd.merge(df_all,
                         df_year,
                         how = 'outer',
                         left_on = 'id_nfl',
                         right_on = 'id_nfl',
                         suffixes = ('_all', '_year'))
    
    # identify players in new (yearly) data that aren't already in old data
    list_new = [x for x in list(df_year['id_nfl']) if x not in list(df_all['id_nfl'])]
    for pid in list_new:
        print('New player found: %s' % pid)
    
    #---------- Handle Duplicate Columns -------------------------------------#
    # fill in variables to account for overlap across the two files
    for variable in df_all.columns.tolist():
        # skip NFL ID
        if variable == 'id_nfl':
            pass
        # keep the _ALL position value unless it's missing, then go with _YEAR
        elif 'pos' in variable:
            df_merged[variable] = df_merged.apply(lambda row:
                row['%s_year' % variable] if (pd.isna(row['%s_all' % variable]) 
                or row['%s_all' % variable] == '') else 
                row['%s_all' % variable], axis = 1) 
        # keep the _YEAR version unless it is missing, then go with _ALL version
        else:
            df_merged[variable] = df_merged.apply(lambda row:
                row['%s_all' % variable] if (pd.isna(row['%s_year' % variable]) 
                or row['%s_year' % variable] == '') else 
                row['%s_year' % variable], axis = 1) 
        
    # drop _ALL and _YEAR variables once merged variable is created
    for variable in df_all.columns.tolist():
        # skip NFL ID
        if variable == 'id_nfl':
            pass
        else:
            df_merged = df_merged.drop(
                    ['%s_all' % variable, '%s_year' % variable], axis = 1)    

    # drop duplicates
    df_merged = df_merged.drop_duplicates()
    
    # check if we have any players with more than 2 entries
    counts = df_merged['id_nfl'].value_counts()
    count_list = counts[counts >= 2].index.tolist()
    df_dups = df_merged[df_merged['id_nfl'].isin(count_list)]
    list_dups = [x for x in df_dups['id_nfl'].drop_duplicates()]
    for dup in list_dups:
        print('WARNING:  Duplicate entry found for player: %s in NFL metadata' 
              % dup)
            
    # reorder columsn for data
    df_merged = df_merged[[
            'player', 'team_nfl', 'age_nfl', 'pos_nfl', 'pos_nfl_std', 
            'school', 'name_first', 'name_last', 'id_ncaa', 'id_nfl', 
            'from_nfl', 'to_nfl', 'height', 'weight', 'av_career', 
            'av_historic', 'draft_round', 'draft_pick', 'draft_team', 
            'draft_year', 'url_pic_player', 'list_teams']]
    
    # fill NANs with empty strings
    df_merged = df_merged.fillna('')
    
    # write merged file to disk
    df_merged.to_csv('data_scraped/nfl_player_meta/player_lookup_nfl.csv', 
                     index = False)
    
    return

def merge_missing_with_all():
    '''
    Purpose: Merges newly scraped data for 'missing' players with the 
        metadata table containing players from all scraped years.

    Inputs
    ------
        NONE
    
    Outputs
    -------
        player_lookup_nfl.csv : CSV File
            File written to disk that contains metadata on ALL scraped players
    '''
    df_missing = pd.read_csv(
            'data_scraped/nfl_player_meta/player_lookup_nfl_missing.csv')
    df_old= pd.read_csv('data_scraped/nfl_player_meta/player_lookup_nfl.csv')
    
    # backup old file
    df_old.to_csv('data_scraped/nfl_player_meta/player_lookup_nfl_backup.csv', 
                  index = False)
    
    # append 'missing' players to old metadata table
    df_all = df_old.append(df_missing)

    # sort table by player ID
    df_all = df_all.sort_values(by = 'id_nfl') 
    
    # fill NANs with empty strings
    df_all = df_all.fillna('')
    
    # write merged file to disk
    df_all.to_csv('data_scraped/nfl_player_meta/player_lookup_nfl.csv', 
                     index = False)
    
    return

def scrape_players(scrape_type, year = 0):
    '''
    Purpose: Scrapes player metadata from pro-football-reference.com for all
        NFL player or only those in the specified year (per variable `year`)

    Inputs
    ------
        scrape_type : string
            'all' --> scrape data for all players for whom elo data exists
            'year' --> scrape data for only those players found in a specified
                year's elo data
            'missing' --> players in the stats data who do not have values in
                the nfl_metadata table
        year : int
            Year of data which is to be scraped (only required if 'year' is 
            specified in `scrape_type`)
    
    Outputs
    -------
        player_lookup_nfl_xxxx.csv : CSV File
            File written to disk that contains metadata on scraped players
    '''
    # set the file name for active players and ingest player IDs for scraping
    if scrape_type == 'all':
        filename = 'data_scraped/nfl_player_meta/player_lookup_nfl_all.csv'
        list_player_ids = []
        for file in sorted(glob.glob('data_scraped/nfl_player/*.csv')):
            list_ids_year = list(pd.read_csv(file)['unique_id'])
            list_player_ids = sorted(list(set(list_player_ids + list_ids_year)))
    elif scrape_type == 'year':
        filename = ('data_scraped/nfl_player_meta/player_lookup_nfl_%s.csv' % 
                    str(year))
        # sort and dedup list
        list_player_ids = sorted(list(set(pd.read_csv(
            'data_scraped/nfl_player/nfl_player_%s.csv' % str(year))['unique_id'])))
    elif scrape_type == 'missing':
        filename = ('data_scraped/nfl_player_meta/player_lookup_nfl_missing.csv')
        df_meta = pd.read_csv('data_scraped/nfl_player_meta/player_lookup_nfl.csv')

        list_missing = []
        for file in tqdm.tqdm(glob.glob('data_scraped/nfl_player/*.csv')):
            df_file = pd.read_csv(file)
            for pid in list(df_file['unique_id']):
                if pid not in list_missing:
                    list_missing.append(pid)
                    
        list_player_ids = sorted([x for x in list_missing if x not in list(
                df_meta['id_nfl'])])
    else:
        print('Incorrect value for scrape-type, please try again.')
        return
    
    # set the column headers for the scraped data
    headers = ['player', 'team_nfl', 'age_nfl', 'pos_nfl', 'pos_nfl_std', 
               'school', 'name_first', 'name_last', 'id_ncaa', 'id_nfl', 
               'from_nfl', 'to_nfl', 'height', 'weight', 'av_career', 
               'av_historic', 'draft_round', 'draft_pick', 'draft_team', 
               'draft_year', 'url_pic_player', 'list_teams']   
    
    # write data to a csv file as it is scraped
    with open(filename, 'w') as f:
        # Initialize the CSV file writer
        wr = csv.writer(f)
        
        # Write the headers of the CSV
        wr.writerow(headers)

        # for every player, query ProFootballReference and get their metadata      
        for player_id in tqdm.tqdm(list_player_ids):
            # check for missing player_ids
            if pd.isna(player_id):
                continue
            
            # scrape a player's profile page
            url = ('https://www.pro-football-reference.com/players/%s/%s.htm' % 
                   (player_id[0].upper(), player_id))
            soup = soupify_url(url)
        
            # Ensure data has been found and no '404 error' exists:
            if 'Page Not Found (404 error)' in str(soup):
                dict_player = {}
                dict_player['unique_id'] = player_id
            else:
                dict_player = process_player_info(soup, player_id)
                
                # remove all NaNs
                for key in dict_player.keys():
                    # replace NaNs in arrays with blank strings
                    if key == 'av_historic':
                        av_historic_new = []
                        for av in dict_player[key]:
                            if pd.isna(av):
                                av_historic_new.append('')
                            else:
                                av_historic_new.append(av)
                        dict_player[key] = av_historic_new
                        
                    # replace NaN value with a blank string
                    elif type(dict_player[key]) != list:
                        if pd.isna(dict_player[key]):
                            dict_player[key] = ''
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
    elif scrape_type == 'missing':
        merge_missing_with_all()

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
#scrape_players('year', 2019)

## ingest the unique IDs of the players in the most recent version of the ELO data
#list_player_ids = list(pd.read_csv('data_elo/nfl_elo.csv')['unique_id'])
#
## scrape player info
#scrape_players(list_player_ids)
#scrape_players('all')