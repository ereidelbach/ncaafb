#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 11:08:58 2019

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
import glob
import os  
import pandas as pd
import pathlib
import tqdm

#==============================================================================
# Reference Variable Declaration
#==============================================================================

#==============================================================================
# Function Definitions
#==============================================================================
def function_name(var1, var2):
    '''
    Purpose: Stuff goes here

    Inputs   
    ------
        var1 : type
            description
        var2 : type
            description
            
    Outputs
    -------
        var1 : type
            description
        var2 : type
            description
    '''
#==============================================================================
# Working Code
#==============================================================================

# Set the project working directory
path_dir = pathlib.Path('/home/ejreidelbach/Projects/CollegeFootball')
os.chdir(path_dir)

# ingest all ncaa data
df_all = pd.DataFrame()
for filename in sorted(glob.glob('Data/sports_ref/ncaa_schedules/*.csv')):
    if len(df_all) == 0:
        df_all = pd.read_csv(filename)
    df_all = df_all.append(pd.read_csv(filename))
    
# ingest ncaa team metadata
df_meta = pd.read_csv('Data/teams_ncaa.csv', usecols = ['Team', 'FBS'])
dict_meta = df_meta.set_index('Team').T.to_dict('index')['FBS']

# only keep games in which the away team is an FBS team
list_rows = []
for index, row in df_all.iterrows():
    if dict_meta[row['away_team']] == True:
        list_rows.append(row)
df_games = pd.DataFrame(list_rows)
    
# exclude neutral site games
df_games = df_games[df_games['neutral_site'] == False]

# drop games that have not been played
df_games = df_games.dropna(subset = ['home_points', 'away_points'])
df_games = df_games.drop_duplicates()

# iterate over each team's away games
list_teams = sorted(list(set(df_games['away_team'])))

# find the longest streak of giving up more than 30 points (of only FBS teams)
dict_streak = {}
for team in tqdm.tqdm(list_teams):
    # isolate the games in which the team was the road team
    df_team = df_games[df_games['away_team'] == team].reset_index(drop = True)
    
    # initialize streak tracking values
    streak = 0
    dict_streak[team] = 0
    
    # iterate over all games and keep a count of the longest streak 
    for index, row in df_team.iterrows():
        # if the score is greater than 0, increase the count
        if row['home_points'] >= 31:
            streak = streak + 1
        # handle the streak ending
        else:
            # check if the new streak is longer than the older streak
            if dict_streak[team] > streak:
                pass
            else:
                dict_streak[team] = streak
            # reset the streak
            streak = 0
    
    # one final check of the streak, now that we're done
    if dict_streak[team] < streak:
        dict_streak[team] = streak
        print(f'{team} is on an active streak of {streak} games')