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
def ingest_data(league):
    '''
    Read in all team-based data for the desired league

    Inputs   
    ------
        league : string
            'nfl' or 'ncaa'
            
    Outputs
    -------
        df_all : Pandas DataFrame
            contains data for all available years
    '''
    # find files
    files = glob.glob(f'data_scraped/{league}_team/*.csv')
    
    # read in all data
    df_all = pd.DataFrame()
    for file in files:
        if len(df_all) == 0:
            df_all = pd.read_csv(file)
        else:
            df_all = df_all.append(pd.read_csv(file), sort = False)

    # if ncaa data, add conference and power five columns
    if league == 'ncaa':
        # read in team data
        df_team_meta = pd.read_csv('data_team/teams_ncaa.csv', 
                                   usecols = ['Team', 'Power5', 'Conference'])
        df_team_meta = df_team_meta.rename(columns = {'Team':'School'})
        
        # add conference and power five columns
        df_all = pd.merge(df_all, 
                            df_team_meta, 
                            how = 'left', 
                            on = 'School')
            
    return df_all

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

# read in college data
df = ingest_data('ncaa')
        
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