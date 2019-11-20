#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 27 10:24:08 2019

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
import ast
import json
import numpy as np
import os  
import pandas as pd
import pathlib

#==============================================================================
# Reference Variable Declaration
#==============================================================================

#==============================================================================
# Function Definitions
#==============================================================================
def rename_school(df, name_var):
    '''
    Purpose: Rename a school/university to a standard name as specified in 
        the file `teams_ncaa.csv`

    Inputs
    ------
        df : Pandas Dataframe
            DataFrame containing a school-name variable
        name_var : string
            Name of the variable which is to be renamed/standardized
    
    Outputs
    -------
        df : Pandas Dataframe
            DataFrame containing the standardized team name 
    '''  
    # read in school name information
    df_school_names = pd.read_csv('data_team/teams_ncaa.csv')
     
    # convert the dataframe to a dictionary such that the keys are the
    #   optional spelling of each school and the value is the standardized
    #   name of the school
    dict_school_names = {}
    
    for index, row in df_school_names.iterrows():
        # isolate the alternative name columns
        names = row[[x for x in row.index if 'Name' in x]]
        # convert the row to a list that doesn't include NaN values
        list_names = [x.strip() for x in names.values.tolist() if str(x) != 'nan']
        # add the nickname to the team names as an alternative name
        nickname = row['Nickname'].strip()
        list_names_nicknames = list_names.copy()
        for name in list_names:
            list_names_nicknames.append(name + ' ' + nickname)
        # extract the standardized team name
        name_standardized = row['Team'].strip()
        # add the standardized name
        list_names_nicknames.append(name_standardized)
        # add the nickname to the standardized name
        list_names_nicknames.append(name_standardized + ' ' + nickname)
        # for every alternative spelling of the team, set the value to be
        #   the standardized name
        for name_alternate in list_names_nicknames:
            dict_school_names[name_alternate] = name_standardized
            
    def swapSchoolName(name_old):
        if ((name_old == 'nan') or (pd.isna(name_old)) or 
             (name_old == 'none') or (name_old == '')):
            return ''
        try:
            return dict_school_names[name_old]
        except:
            print('Did not find: %s' % (name_old))
            return name_old
            
#    df[name_var] = df[name_var].apply(
#            lambda x: dict_school_names[x] if str(x) != 'nan' else '')
    df[name_var] = df[name_var].apply(lambda x: swapSchoolName(x))
    
    return df  

def rename_nfl(df, name_var):
    '''
    Purpose: Rename an NFL team to a standardized name as specified in 
        the file `teams_nfl.csv`

    Inputs
    ------
        df : Pandas Dataframe
            DataFrame containing an NFL-name variable
        name_var : string
            Name of the variable which is to be renamed/standardized
    
    Outputs
    -------
        df : Pandas Dataframe
            DataFrame containing the standardized team name
    '''  
    # read in school name information
    df_team_names = pd.read_csv('data_team/teams_nfl.csv')
     
    # convert the dataframe to a dictionary such that the keys are the
    #   optional spelling of each team and the value is the standardized
    #   name of the team
    dict_team_names = {}
    
    for index, row in df_team_names.iterrows():
        # isolate the alternative name columns
        names = row[[x for x in row.index if 'name' in x.lower()]]
        # convert the row to a list that doesn't include NaN values
        list_names_nicknames = [
                x for x in names.values.tolist() if str(x) != 'nan']
        # add the abbreviated name
        list_names_nicknames.append(row['Team'])
        # set the standardized name to be used
        name_standardized = row['FullName']
        # for every alternative spelling of the team, set the value to be
        #   the standardized name
        for name_alternate in list_names_nicknames:
            dict_team_names[name_alternate] = name_standardized
            
    def swapteamName(name_old):
        if ((name_old == 'nan') or (pd.isna(name_old)) or 
             (name_old == 'none') or (name_old == '')):
            return ''
        try:
            return dict_team_names[name_old]
        except:
            print('Did not find: %s' % (name_old))
            return name_old
            
    df[name_var] = df[name_var].apply(lambda x: swapteamName(x))
    
    return df   
    
def set_team_code(list_teams, team_type):
    '''
    Purpose: Determine the NFL or NCAA team code for each player's respective
        school and NFL team (if they exist)

    Inputs
    ------
        list_teams : list of strings
            list of teams that require team codes
        team_type : string
            'NFL' or 'NCAA' -- type of team being evaluated

    Outputs
    -------
        list_codes : list of ints
            list of codes for each team
    '''
    # read in team dataframe
    if team_type == 'ncaa':
        df_teams = pd.read_csv('data_team/teams_ncaa.csv')
    elif team_type == 'nfl':
        df_teams = pd.read_csv('data_team/teams_nfl.csv')
    else:
        print('Incorrect team type passed in...please try again')
        return

    # convert the dataframe to a dictionary with keys as names and values as codes
    dict_teams = {}
    for index, row in df_teams.iterrows():
        # isolate the alternative name columns
        names = row[[x for x in row.index if 'Name' in x]]
        # convert the row to a list that doesn't include NaN values
        list_names = [x.strip() for x in names.values.tolist() if str(x) != 'nan']
        # add the nickname to the team names as an alternative name
        nickname = row['Nickname'].strip()
        list_names_nicknames = list_names.copy()
        for name in list_names:
            list_names_nicknames.append(name + ' ' + nickname)
        # extract the standardized team name
        name_standardized = row['Team'].strip()
        # add the standardized name
        list_names_nicknames.append(name_standardized)
        # add the nickname to the standardized name
        list_names_nicknames.append(name_standardized + ' ' + nickname)
        # extract the team code
        team_code = row['TeamCode']
        # for every alternative spelling of the team, set the value to be
        #   the standardized name
        for name_alternate in list_names_nicknames:
            dict_teams[name_alternate] = team_code
    
    list_codes = []
    for team in list_teams:
        try:
            list_codes.append(dict_teams[team])
        except:
            list_codes.append(np.nan)
        
    return list_codes    

def insert_combine_data(df_meta):
    '''
    Purpose: Inserts combine information into metadata table

    Inputs
    ------
        df_meta : Pandas Dataframe
            DataFrame containing player metadata (both NCAA and NFL information)
    
    Outputs
    -------
        df_merged : Pandas Dataframe
            DataFrame containing player metadata freshly infused with combine info
    '''
    # read in combine information
    df_combine = pd.read_csv('data_combine/combine_all_years.csv')
    
    # isolate desired combine information
    df_combine = df_combine.drop(columns = {'combine_year', 'drafted'})
    
    #---------------- Merge on ID_NCAA ---------------------------------------#
    df_merged = pd.merge(df_meta,
                         df_combine,
                         how = 'left',
                         left_on = 'id_ncaa',
                         right_on = 'id_ncaa')
    df_merged = df_merged.rename(columns = {'id_nfl_x' : 'id_nfl'})
    df_merged = df_merged.drop(columns = {'id_nfl_y'})
    
    #---------------- Merge on ID_NFL ----------------------------------------#
    df_merged = pd.merge(df_merged,
                         df_combine,
                         how = 'left',
                         left_on = 'id_nfl',
                         right_on = 'id_nfl')
    df_merged = df_merged.rename(columns = {'id_ncaa_x' : 'id_ncaa'})
    df_merged = df_merged.drop(columns = {'id_ncaa_y'})
    
    #----------------- Combine Duplicate Columns -----------------------------#
    
    # Combine duplicate variables
    for variable in ['combine_height',
                     'combine_weight',
                     'combine_40yd',
                     'combine_vertical',
                     'combine_bench',
                     'combine_broad',
                     'combine_3cone',
                     'combine_shuttle']:
        # keep the _x version unless it is missing, then go with _y
        df_merged[variable] = df_merged.apply(lambda row:
            row['%s_y' % variable] if (pd.isna(row['%s_x' % variable]) 
            or row['%s_x' % variable] == '') else 
            row['%s_x' % variable], axis = 1)     
            
    # Delete duplicate variables
    df_merged = df_merged.drop(columns = {
            'combine_height_x', 'combine_height_y',
            'combine_weight_x', 'combine_weight_y',
            'combine_40yd_x', 'combine_40yd_y',
            'combine_vertical_x', 'combine_vertical_y',
            'combine_bench_x', 'combine_bench_y',
            'combine_broad_x', 'combine_broad_y',
            'combine_3cone_x', 'combine_3cone_y',
            'combine_shuttle_x', 'combine_shuttle_y'})
    
    # Merge height with combine_height
    df_merged['height'] = df_merged.apply(lambda row:
        row['combine_height'] if (pd.isna(row['height']) or row['height'] == '') 
        else row['height'], axis = 1)
    # Merge weight with combine_weight
    df_merged['weight'] = df_merged.apply(lambda row:
        row['combine_weight'] if (pd.isna(row['weight']) or row['weight'] == '') 
        else row['weight'], axis = 1)
        
    # Delete combine height / weight variables
    df_merged = df_merged.drop(columns = {
            'combine_height', 'combine_weight'})
    
    return df_merged
    
def merge_ncaa_and_nfl_metadata():
    '''
    Purpose: Merges the individual metadata files (for NCAA and NFL data) into
        one cohesive file for future export/ingest into the DraftGem database

    Inputs
    ------
        NONE
    
    Outputs
    -------
        player_lookup.csv : csv file
        player_lookup.json : json file
            Local copies of all player metadata (inclues both NCAA and NFL data)
    '''    
    #---------- Ingest and Merge Data ----------------------------------------#
    # ingest raw folders
    df_ncaa = pd.read_csv('data_scraped/ncaa_player_meta/player_lookup_ncaa.csv')
    df_nfl = pd.read_csv('data_scraped/nfl_player_meta/player_lookup_nfl.csv')
    
    # rename `list_teams` variables for both files
    df_ncaa = df_ncaa.rename(columns = {'list_schools':'list_teams_ncaa'})
    df_nfl = df_nfl.rename(columns = {'list_teams':'list_teams_nfl'})

    # merge NFL and NCAA data
    df_merged = pd.merge(df_ncaa, 
                         df_nfl,
                         how = 'outer',
                         left_on = ['id_ncaa'],
                         right_on = ['id_ncaa'],
                         suffixes = ('_NCAA', '_NFL'))
    
    #---------- Format Columns as Desired ------------------------------------#
    # standardize all NCAA school names
    df_merged = rename_school(df_merged, 'school_NCAA')
    df_merged = rename_school(df_merged, 'school_NFL')
    
    # standardize all NFL team names
    df_merged = rename_nfl(df_merged, 'team_nfl')
    
    #---------- Handle Duplicate Columns -------------------------------------#
    # fill in variables to account for overlap across the two files
    for variable in ['draft_pick', 'draft_round', 'draft_team', 'draft_year', 
                     'height', 'name_first', 'name_last', 'player', 'school', 
                     'weight', 'url_pic_player']:
        # keep the _NCAA version unless it is missing, then go with _NFL
        df_merged[variable] = df_merged.apply(lambda row:
            row['%s_NFL' % variable] if (pd.isna(row['%s_NCAA' % variable]) 
            or row['%s_NCAA' % variable] == '') else 
            row['%s_NCAA' % variable], axis = 1) 
        
    # drop _NCAA and _NFL variables once merged variable is created
    for variable in ['draft_pick', 'draft_round', 'draft_team', 'draft_year', 
                     'height', 'name_first', 'name_last', 'player', 'school', 
                     'weight', 'url_pic_player']:
        df_merged = df_merged.drop(
                ['%s_NCAA' % variable, '%s_NFL' % variable], axis = 1)
        
    # Ensure position variables are properly stored as lists of strings
    list_positions = [x for x in list(df_merged.columns) if 'pos' in x.lower()]
    for position in list_positions:
        list_temp = []
        for row in df_merged[position]:
            if pd.isna(row):
                list_temp.append([])
            else:
                try:
                    row = ast.literal_eval(row)
                    list_temp.append([str(x) for x in row])
                except:
                    print(position + ': ' + row)
        df_merged[position] = list_temp
        
    # Ensure `list of teams` variables are properly stored as lists of tuples
    for column in ['list_teams_ncaa', 'list_teams_nfl']:
        df_merged.apply(
                lambda row: ast.literal_eval(row[column]) if not pd.isna(
                        row[column]) else [], axis = 1)
        
    # Ensure AV_historic is properly stored as a list of ints
    list_temp = []
    for row in df_merged['av_historic']:
        if pd.isna(row):
            list_temp.append(np.nan)
        else:
            try:
                row = row.replace('nan','0')
                row = ast.literal_eval(row)
                list_temp.append([int(x) if ~pd.isna(x) else 0 for x in row])
            except:
                print(row)
    df_merged['av_historic'] = list_temp
            
    # insert team codes for players (NFL and NCAA)
    df_merged['school_code'] = set_team_code(df_merged['school'].tolist(), 'ncaa')
    df_merged['team_nfl_code'] = set_team_code(df_merged['team_nfl'].tolist(), 'nfl')
    
    # insert player combine information
    df_merged = insert_combine_data(df_merged)
    
    # reorder columns as desired
    df_merged = df_merged[[
            'player', 'name_first', 'name_last', 'school', 'school_code', 
            'team_nfl', 'team_nfl_code', 'age_nfl', 'height', 'weight', 
            'pos_ncaa', 'pos_ncaa_std', 'id_ncaa', 'from_ncaa', 'to_ncaa', 
            'pos_nfl', 'pos_nfl_std', 'id_nfl', 'from_nfl', 'to_nfl', 
            'av_career', 'av_historic', 'draft_pick', 'draft_round', 
            'draft_team', 'draft_year', 'combine_vertical', 'combine_bench', 
            'combine_broad', 'combine_3cone', 'combine_shuttle', 'combine_40yd', 
            'url_pic_player', 'list_teams_nfl', 'list_teams_ncaa']]

    # write csv to disk
    df_merged.to_csv('data_player/player_lookup.csv', index = False)    

    # fill in missing values with blanks rather than float NaNs
    df_merged = df_merged.fillna('')
    
    # Convert the dataframe to a dictionary
    dict_merged = df_merged.to_dict('records')    
    
    # remove keys with empty values in each dictionary
    for index, metadata in enumerate(dict_merged):
        # remove keys
        dict_reduced = {key: value for key, value in metadata.items() if value}
        # overwrite data in list
        dict_merged[index] = dict_reduced
    
    # Write dictionary to a .json file
    with open('data_player/player_lookup.json', 'wt') as out:
            json.dump(dict_merged, out, sort_keys=True) 

def main():
    pass

#=============================================================================
# Working Code
#==============================================================================
if __name__ == "__main__":
    main()

## Set the project working directory
#path_dir = pathlib.Path('/home/ejreidelbach/Projects/draft-analytics-data')
##path_dir = pathlib.Path('D:\DataScience\Projects\draft-analytics-data')
#os.chdir(path_dir)
#
#merge_ncaa_and_nfl_metadata()