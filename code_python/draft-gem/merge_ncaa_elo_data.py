#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 16:18:13 2019

@author: ejreidelbach

:DESCRIPTION: Ingests newly created NCAA elo data and formats it in the desired 
    manner for subsequent ingest by DraftGeM API

:REQUIRES:
    Refer to the Package Import section of the script
    
:TODO:
    TBD
"""
 
#==============================================================================
# Package Import
#==============================================================================
import datetime
import json
import os  
import pandas as pd
import pathlib
import tqdm
#from ast import literal_eval

#==============================================================================
# Reference Variable Declaration
#==============================================================================
dict_variables_to_fix = {
        'QB': ['qbrElo', 'ypgElo', 'ypcElo', 'ypaElo', 'pctElo', 'tdElo', 
               'intElo', 'eloC', 'date', 'opp'],
        'RB': ['ypgElo', 'ypcElo', 'yfsElo', 'tdElo', 'eloC', 'date', 'opp'],
        'WR': ['recElo', 'ypgElo', 'ypcElo', 'tdElo', 'eloC', 'date', 'opp'],
        'DEF': ['tacklesElo', 'tflElo', 'sackElo', 'intElo', 'pbuElo', 'ffElo', 
                'date', 'opp']
        }

#==============================================================================
# Function Definitions
#==============================================================================      
def merge_elo_data():
    '''
    Purpose: Current versions of ELO data are supplied in multiple files, one
        for each statistical category (passing, rushing, receiving and defense).
        This function will merge those files together, rename variables to make
        them easier to distinguish from one another, and then ensure all 
        variables of type 'list' are being stored correctly.
        
    Inputs
    ------
        NONE
            
    Outputs
    -------
        df_master : Pandas DataFrame
            A dataframe containing all ELO data spanning all 4 categories
    '''
    # create a master dataframe for storing the data
    df_master = pd.DataFrame()
    for pos in tqdm.tqdm(['QB', 'RB', 'WR', 'DEF']):
        # create a dictionary of converter statements that will be used to handle
        #   the ingest for all variables with an array
        dict_converters = {}
        
        # associate the variables with their converter function
        for column in dict_variables_to_fix[pos]:
            dict_converters[column] = lambda x: x.strip("[]").split(", ")
        
        # read in the data, correctly, this time
        df = pd.read_csv('data_raw/cfb_%s.csv' % pos, 
                                 converters = dict_converters)
        
        # rename variables to separate variables across position types
        list_columns = []
        for column in df.columns.tolist():
            if column not in ['unique_id']:
                list_columns.append('%s_%s' % (pos, column))
            else:
                list_columns.append(column)
        df.columns = list_columns
        
        # add to the master dataframe
        if len(df_master) == 0:
            df_master = df.copy()
        else:
            df_master = pd.merge(df_master, 
                                 df, 
                                 how = 'outer', 
                                 on = ['unique_id'])

    # Make sure columns with ELO data are stored as lists of ints
    # obtain a list of all elo variables
    list_elo = [x for x in list(df_master.columns) if 'elo' in x.lower()]
    
    # iterate through each elo variable and eval it so that it's a list
    for elo in list_elo:
        list_temp = []
        # for each row in the variable column, convert list to list of ints
        for row in df_master[elo]:
            list_row = []
            if type(row) != list and pd.isna(row):
                list_temp.append('')
            else:
                for x in row:
                    x = x.replace("'","")   # handle default string formatting
                    if x == "NA":           # handle week 0 by making NA -> 0
                        list_row.append(0)
                    else:
                        list_row.append(int(x))
                list_temp.append(list_row)
        df_master[elo] = list_temp
        
    # Make sure opponent ids are stored as list of ints 
    list_opp = [x for x in list(df_master.columns) if 'opp' in x.lower()]
    # iterate over every opponent variable
    for opp in list_opp:
        list_temp = []
        # for each row in the variable column, convert list to list of ints
        for row in df_master[opp]:
            list_row = []
            if type(row) != list and pd.isna(row):
                list_temp.append('')
            else:
                for x in row:
                    x = x.replace("'","")   # handle default string formatting
                    if x == "NA":           # handle week 0 by making NA -> 0
                        list_row.append(0)
                    else:
                        list_row.append(int(x))
                list_temp.append(list_row)
        df_master[opp] = list_temp
    
    # Make sure the date column is stored as a list of strings
    list_date = [x for x in list(df_master.columns) if 'date' in x.lower()]
    for date in list_date:
        list_temp = []
        # for each row in the variable column, convert list to list of strings
        for row in df_master[date]:
            list_row = []
            if type(row) != list and pd.isna(row):
                list_temp.append('')
            else:
                for x in row:
                    x = x.replace("'","")       # handle default string formatting
                    if row == '':
                        list_row.append("")
                    else:
                        list_row.append(str(x))
                list_temp.append(list_row)
        df_master[date] = list_temp

    # remove the following unnecessary variabes:
    #   - all win-related variables ('xxx_w')
    #   - all loss-related variables ('xxx_l')
    #   - all last-related variables ('xxx_last')
    # keep the following 'xxx_last' variables for sorting purposes
    #   - RB_last
    #   - QB_Last
    #   - WR_last
    #   - DEF_lastPBU
    #   - DEF_lastTackles
    #   - DEF_lastTFL
    list_cols_keep = []
    for column in df_master.columns:
        if column in ['RB_last', 'QB_last', 'WR_last', 'DEF_lastPBU', 
                      'DEF_lastTackles', 'DEF_lastTFL']:
            list_cols_keep.append(column)
        elif not any(substring in column for substring in ['_w', '_l', '_last']):
            list_cols_keep.append(column)
    
    # move the player ID column to the front of the table for ease of reference
    list_cols_keep.insert(0, list_cols_keep.pop(list_cols_keep.index('unique_id')))

    # sort the dataframe
    df_master = df_master[list_cols_keep]

    # create a `last_year` flag for players who have played a game within the last year
    list_last_year = []
    for index, row in tqdm.tqdm(df_master.iterrows()):
        # get the last date played for each position
        if row['QB_date'] != '':
            date_QB = datetime.datetime.strptime(row['QB_date'][-1], '%Y%m%d')
        else:
            date_QB = datetime.datetime(1900,1,1)
        if row['RB_date'] != '':
            date_RB = datetime.datetime.strptime(row['RB_date'][-1], '%Y%m%d')
        else:
            date_RB = datetime.datetime(1900,1,1)
        if row['WR_date'] != '':
            date_WR = datetime.datetime.strptime(row['WR_date'][-1], '%Y%m%d')
        else:
            date_WR = datetime.datetime(1900,1,1)
        if row['DEF_date'] != '':
            date_DEF = datetime.datetime.strptime(row['DEF_date'][-1], '%Y%m%d') 
        else:
            date_DEF = datetime.datetime(1900,1,1)
        
        # get the most recent of all dates
        date_recent = pd.Series(
                [date_QB, date_RB, date_WR, date_DEF]).sort_values(
                        ascending = False).iloc[0]
        
        # if the player last played more than two season ago, they are not active
        if ((datetime.datetime.today() - date_recent).days > 450):
            list_last_year.append(False)
        else:
            list_last_year.append(True)
    df_master['last_year']  = list_last_year
                    
    # only retain players who have played 8 or more games or are active
    df_master = df_master[(df_master['DEF_count'] > 8) |
                      (df_master['QB_count'] > 8) |
                      (df_master['RB_count'] > 8) |
                      (df_master['WR_count'] > 8) | 
                      (df_master['last_year'] == True)]
    
    # clear out entries for players who aren't active (i.e. haven't played
    #   a game within more than 450 years -- more than 2 season) and have
    #   less than 8 games in the specific position category 
    # this removes QBs who have a few receivers stats a single tackle score
    list_rows = []
    for index, row in tqdm.tqdm(df_master.iterrows()):
        for position in ['QB', 'RB', 'WR', 'DEF']:
            # reset position info to missing if not enough games or old player
            if (((row[position + '_count'] <= 7) or (pd.isna(row[position + '_count'])))
                and (row['last_year'] != True)):
                columns = [x for x in df_master.columns if position in x]
                for column in columns:
                    row[column] = ''
        list_rows.append(row)
    df_master = pd.DataFrame(list_rows)
                
    # fill in NaNs with empty strings
    df_master = df_master.fillna('')
    
    # drop the `last_year` column as it is no longer needed
    df_master = df_master.drop(columns = ['last_year'])
    
    # read in player metadata in order to insert 'position', `position_std` and `to`
    df_meta = pd.read_csv('data_player/player_lookup.csv')
    df_meta = df_meta[['id_ncaa', 'pos_ncaa', 'pos_ncaa_std', 'to_ncaa']]
    df_meta = df_meta.rename(columns = {'id_ncaa':'unique_id',
                                        'pos_ncaa':'position',
                                        'pos_ncaa_std':'position_std',
                                        'to_ncaa':'To'})

    # merge 'position', 'position_std' and 'to' into the elo data
    df_merged = pd.merge(df_master,
                         df_meta,
                         how = 'left',
                         left_on = 'unique_id',
                         right_on = 'unique_id')

    # fill in missing values with blanks rather than float NaNs
    df_merged = df_merged.fillna('')
    
    # export csv to disk
    df_merged.to_csv('data_elo/college_elo.csv', index = False)
    
    # Convert the dataframe to a dictionary
    dict_meta = df_merged.to_dict('records')    
    
    # remove keys with empty values in each dictionary
    for index, metadata in enumerate(dict_meta):
        # remove keys
        dict_reduced = {key: value for key, value in metadata.items() if value}
        # overwrite data in list
        dict_meta[index] = dict_reduced
    
    # Write dictionary to a .json file
    with open('data_elo/college_elo.json', 'wt') as out:
            json.dump(dict_meta, out, sort_keys=True) 

    return 

def main():
    pass

#=============================================================================
# Working Code
#==============================================================================
if __name__ == "__main__":
    main()
    
## Set the project working directory
#path_dir = pathlib.Path('/home/ejreidelbach/Projects/draft-analytics-data')
#os.chdir(path_dir)
#
## merge all the raw elo data together into one file
#merge_elo_data()