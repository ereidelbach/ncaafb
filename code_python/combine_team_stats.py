#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 16 13:10:10 2018

@author: ejreidelbach

:DESCRIPTION:

:REQUIRES:
   
:TODO:
"""
 
#==============================================================================
# Package Import
#==============================================================================
import glob
import os  
import pandas as pd
import pathlib

#==============================================================================
# Reference Variable Declaration
#==============================================================================
list_vars_offense_game = ['date', 'opponent', 'opp_rank', 'home_away', 'result',
                          'pts_for', 'pts_against', 'pts_diff', 'surface', 
                          'month', 'day', 'year', 'day_of_week', 'season']

#==============================================================================
# Function Definitions
#==============================================================================
def function_name(var1, var2, var3):
    '''
    Purpose: Stuff goes here

    Input:   
        (1) var1 (type): description
        (2) var2 (type): description
        (3) var3 (type): description
    
    Output: 
        (1) output1 (type): description
    '''
    
def mergeOffenseGameStats(dir_path):
    '''
    Purpose: Ingest all .csv stat files ending in _offense_game and merge into
        one, combined DataFrame before exporting to a comprehensive .csv file.
        
    Input:
        - dir_path (pathlib.Path): Directory path of CFB Stats Data folder
        
    Output:
        - (.csv) Comprehensive .csv file containing all Offensive Game stats
    '''
    # Iterate over every team's folder
    df_master = pd.DataFrame()
    for folder_team in dir_path.iterdir():
        # Iterate over every offensive game file in the team's directory
        for file_name in folder_team.glob('*offense_game.csv'):
            # Ingest the file
            df_file = pd.read_csv(file_name)
            
            # Create a list of all variable names in the DataFrame
            list_column_names = list(df_file.columns)
            
            # Determine the category of the file we're working with
            category = str(file_name).split('.csv')[0].split(
                    '/')[-1].split('_offense_game')[0]            
            
            # Adjust variable names to better identify them
            for name in list_column_names:
                if name not in list_vars_offense_game:
                    # Append the category to the front of the variable name
                    name_new = category + '_' + name
                    # Rename the variable in the list
                    list_column_names[list_column_names.index(name)] = name_new
            
            # Overwrite the existing variable names
            df_file.columns = list_column_names
                
            # If the master dataframe is empty, initialize it to the contents
            #   of the first file we're ingesting from the folder
            if df_master.empty == True:
                df_master = df_file.copy()
            # Otherwise, left join the data only adding new variables to master
            else:
                df_master = df_master.merge(df_file, how='left')
                
    # Export master DataFrame to a .csv file
    
    
#==============================================================================
# Working Code
#==============================================================================

# Set the project working directory
dir_path = pathlib.Path(
        '/home/ejreidelbach/Projects/CollegeFootball/Data/CFBStats')

# Merge Offense/Defense - Game stats

# Merge Offense/Defense - Split stats

# Merge Offense/Defense - Situational stats

# Merge Team/Opponent - Game stats

# Merge Team/Opponent - Split stats

# Merge Other Team - Game stats

# Merge Other Team - Split stats