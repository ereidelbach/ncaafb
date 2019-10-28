#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 26 13:04:19 2019

@author: ejreidelbach

:DESCRIPTION: Contains functions that are used to standardize team names 
    and team logos for all NCAA and NFL teams.

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

#==============================================================================
# Reference Variable Declaration
#==============================================================================

#==============================================================================
# Function Definitions
#==============================================================================
def build_team_dict(league, value = 'Team'):
    '''
    Purpose: Create a lookup dictionary for teams at the NCAA or NFL level
    
    Inputs
    ------
        league : string
            The type of league to build the dictionary for (i.e. NCAA or NFL)
        value : string
            Name of variable that should serve as the value in the key-value pair
            
    Outputs
    -------
        dict_team_names : dictionary 
            Team name lookup table with alternate names as keys and the desired
            standardized name as the value
    '''
    # read in school name information
#    df_team_names = pd.read_csv(path_dir.joinpath(
#            'Data/teams_%s.csv' % league))
    df_team_names = pd.read_csv('Data/teams_%s.csv' % league)

    # convert the dataframe to a dictionary such that the keys are the
    #   optional spelling of each team and the value is the standardized
    #   name of the team
    dict_team_names = {}
    
    for index, row in df_team_names.iterrows():
        # isolate the alternative name columns
        names = row[[x for x in row.index if (('Name' in x) or 
                                              ('TeamCode' in x) or 
                                              ('Team' in x)
                                              )]]

        # convert the row to a list that doesn't include NaN values
        list_names_nicknames = [
                x for x in names.values.tolist() if str(x) != 'nan']
        
        # extract the standardized team name
        name_standardized = row[value]
#        
#        # add the standardized name
#        list_names_nicknames.append(name_standardized)
        
        # for every alternative spelling of the team, set the value to be
        #   the standardized name
        for name_alternate in list_names_nicknames:
            dict_team_names[name_alternate] = name_standardized
            if type(name_alternate) != int:
                dict_team_names[name_alternate + ' ' + row['Nickname']] = name_standardized
    
    return dict_team_names
           
def rename_teams(list_teams, league, std_name = 'Team'):
    '''
    Purpose: Rename a list of teams to standardized name as specified in 
        the file 'Data/teams_nfl.csv' (for NFL teams) or 
        'Data/teams_ncaa.csv' for (NCAA teams)

    Inputs
    ------
        list_teams : list of strings
            Original team names that are to be standardized 
        league : string
            The type of league to build the dictionary for (i.e. NCAA or NFL)  
        std_name : string
            The type of standardized name to return (i.e. a string based 
            standardized name or the numeric TeamCode)
    
    Outputs
    -------
        list_teams_new : list of strings
            Standardized version of the original team names
    '''  
    # retrieve the lookup table for NFL teams
    dict_team_names = build_team_dict(league, std_name)
            
    # function that swaps a given team name for the new, standardized name
    def swap_team_name(name_old):
        if ((name_old == 'nan') or (pd.isna(name_old)) or 
             (name_old == 'none') or (name_old == '')):
            return ''
        try:
            return dict_team_names[name_old]
        except:
            print('Did not find: %s' % (name_old))
            return name_old
    
    # iterate over each team name and swap it out for the standardized version
    list_teams_new = []
    for team in list_teams:
        list_teams_new.append(swap_team_name(team.strip().replace('é','e')))
    
    return list_teams_new

def standardize_logo_ncaa(df):
    '''
    Purpose: Fill in the value of the NCAA logo field for all players in a DF

    Inputs
    ------
        df : Pandas Dataframe
            Contains all the player information and metadata
            
    Outputs
    -------
        urls_ncaa : list of strings
            Standardized version of all school logo URLs
    '''    
    # ingest the school names and URLs from a flat file    
    df_pictures = pd.read_csv('Data/teams_ncaa.csv')

    # create a dictionary where the team name is the key and the url is the value
    df_pictures.set_index('Team', drop=True, inplace=True)
    dict_pictures = df_pictures.to_dict('index')
    for key, value in dict_pictures.items():
        dict_pictures[key] = value['urlSchool']
    
    # create the variable 'pictureSchoolURL' to store each team's logo URL
    df['pictureSchoolURL'] = df['School'].apply(
            lambda x: dict_pictures[x] if x != '' else '')
    
    return df

def standardize_logo_nfl(df):
    '''
    Purpose: Fill in the value of the NFL logo field for all players in a DF

    Inputs
    ------
        df : Pandas Dataframe
            Contains all the player information and metadata
            
    Outputs
    -------
        urls_ncaa : list of strings
            Standardized version of all school logo URLs
    '''    
    # ingest the school names and URLs from a flat file    
    df_pictures = pd.read_csv('Data/teams_nfl.csv')

    # create a dictionary where the team name is the key and the url is the value
    df_pictures.set_index('Team', drop=True, inplace=True)
    dict_pictures = df_pictures.to_dict('index')
    for key, value in dict_pictures.items():
        dict_pictures[key] = value['URL']

    def standardizeName(team):
        try:
            return dict_pictures[team]
        except:
            if team != '':
                print('Logo not found for %s' % (team))
            return ''
    
    # create the variable 'pictureSchoolNFL' to store each team's logo URL
    df['URL'] = df['Tm'].apply(lambda x: standardizeName(x))
    
    return df