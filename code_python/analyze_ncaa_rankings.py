#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 13:52:33 2019

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

# read in team data for all desired years
df = pd.DataFrame()
for year in range(1962, 2020):
    df_year = pd.read_csv(f'Data/sports_ref/ncaa_rankings/ncaa_team_rankings_{year}.csv')
    df_year['year'] = year
    if len(df) == 0:
        df = df_year.copy()
    else:
        df = df.append(df_year, sort = False)
        
# read in team info
df_info = pd.read_csv('Data/teams_ncaa.csv', usecols = ['Team', 'Power5', 'ConferenceAbbrev'])

# merge data
df_merged = pd.merge(df, df_info, how = 'left', left_on = 'School', right_on = 'Team')
df_merged = df_merged.drop(columns = ['Team'])

# subset to Power5 teams and only include 
df_merged.to_csv('Data/sports_ref/historic_rankings.csv', index = False)
