#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 12:24:04 2019

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
import numpy as np
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

'''
 Consider plotting stats using a parallel coordinates chart:
     https://bl.ocks.org/jasondavies/1341281
'''

# Set the project working directory
path_dir = pathlib.Path('/home/ejreidelbach/Projects/CollegeFootball')
os.chdir(path_dir)

# read in team metadata
df_info = pd.read_csv('Data/teams_ncaa.csv', usecols = ['Team', 'Power5', 'Conference'])
df_info = df_info.rename(columns = {'Team':'School'})

# read in team data for all desired years
df = pd.DataFrame()
for year in range(2008, 2020):
    df_year = pd.read_csv(f'Data/sports_ref/ncaa_team/ncaa_team_data_{year}.csv')
    if len(df) == 0:
        df = df_year.copy()
    else:
        df = df.append(df_year)
  
#df_2019 = pd.read_csv(f'Data/sports_ref/ncaa_team/ncaa_team_data_2019.csv')
#df['yds_per_pt'] = df.apply(
#        lambda row: row['Total Yards'] / row['Total Points'] if (
#                row['Total Points']) != 0 else np.nan, axis = 1)

        
# group all games into aggregated team stats
df_team = df.groupby(['School'])['Pass Yard', 'Rush Yard', 'Total Plays', 
                        'Total Yards', 'Total Points'].sum()

# create new variables
df_team['yds_per_pt'] = df_team.apply(
        lambda row: row['Total Yards']  / row['Total Points'], axis = 1)
df_team['plays_per_pt'] = df_team.apply(
        lambda row: row['Total Plays']  / row['Total Points'], axis = 1)
df_team['yds_per_play'] = df_team.apply(
        lambda row: row['Total Yards']  / row['Total Plays'], axis = 1)

# merge statistical data with team info
df_team = pd.merge(df_team, df_info, how = 'left', on = 'School')
df_team = df_team[df_team['Power5'] == True]

# plot conference stats
df_team[df_team['Conference'] == 'Big Ten']['yds_per_pt'].plot(kind = 'hist')