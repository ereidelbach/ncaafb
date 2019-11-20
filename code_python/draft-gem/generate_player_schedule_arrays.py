#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 13:44:13 2019

@author: ejreidelbach

:DESCRIPTION: This script generates schedule arrays for each player based on 
    the schedule of their team for a specific year.  
    
    1. Determine what team a player is on (via player stats)
    2. Compare team schedule vs the games they played
    3. Fill in "empty" games
    4. Determine if a player transfers (compare teams they appear on over
                                        the entirety of their career)
    5. Fill in missing years --> i.e. transfer, full season injury, etc.
    6. Create an array for the date of each game
    7. Create an array with the opponent ID for each game
    8. Create an array with True/False for if they played in the game

:REQUIRES:
    Refer to the Package Import section of the script
    
:TODO:
    TBD
"""
 
#==============================================================================
# Package Import
#==============================================================================
import ast
import csv
import glob
import json
import pandas as pd
import tqdm

#==============================================================================
# Reference Variable Declaration
#==============================================================================

#==============================================================================
# Function Definitions
#==============================================================================
def create_schedule_array(dict_schools, player, df_schedule):
    '''
    Purpose: Given a player's team for each season, create a full array of
        all the games in a player's career and another array that indicates
        if a player played that week or not

    Inputs
    ------
        dict_schools : dictioanry
            Contains a series of year-school key-value pairs for each year in a 
            player's career (i.e. year the player played and the school they 
            played for)
        player : Series
            Contains the dates and opponent ids for every game a player played
            broken out by position (e.g. 'QB', 'RB', 'WR', 'DEF')
        df_schedule : DataFrame
            Contains full schedule of all games on file for the player's league
    
    Outputs
    -------
        player_schedule : Series
            Contains the original player data complete with new values
                - 'ALL_date' : array of dates on which the player's team played
                    a game
                - 'ALL_opp' : array of opponent ID codes for the plaeyr's team's
                    opponent
                - 'play_status' : array of True/False values indicating if a 
                    player played in the game or not
    ''' 
    # initialize player arrays
    player_schedule = {}
    player_schedule['ALL_date'] = []
    player_schedule['ALL_opp'] = []
    player_schedule['play_status'] = []

    #   Note: a player is considered 'eligible' after their first played game
    date_list = []
    for date_array in ['QB_date', 'RB_date', 'WR_date', 'DEF_date']:
        if player[date_array] != '':
            try:
                date_list.append(int(player[date_array][1]))
            except:
                date_list.append(int(player[date_array][1]))
    date_start = str(min(date_list))
     
    # determine the year the player first played
    if date_start[4:6] != '01':
        year_start = int(date_start[0:4])
    else:
        year_start = int(date_start[0:4]) - 1
    
    # determine the last year on file for the player 
    year_last = int(list(sorted(dict_schools.keys()))[-1])
    
    # fill in missing years at the front end of a player's career
    first_team = dict_schools[sorted(dict_schools.keys())[0]]
    for year in range(year_start, year_last + 1):
        if year not in dict_schools.keys():
            dict_schools[year] = first_team

    # iterate over every year the player possibly played (after game 1)
    for year in range(year_start, year_last + 1):
        # isolate the schedule of the player's team for each year they played 
        try:
            team = dict_schools[year]
        except:
            # if a player doesn't play in a season, they remain on the old team
            pass
        df_year = df_schedule[(df_schedule['season'] == year) & (
                (df_schedule['home_team'] == team) | (
                        df_schedule['away_team'] == team))]
    
        # add the team's game dates to the player's list of possible dates
        player_schedule['ALL_date'] = player_schedule['ALL_date'] + list(
                df_year['date'].astype(str))
        # add the team's opp. codes to the player's list of possible opp. codes
        player_schedule['ALL_opp'] =  player_schedule['ALL_opp'] + list(
                df_year.apply(lambda row: row['home_code'] if (
                        team == row['away_team']) else row['away_code'], 
                        axis = 1))

    # create a True/False array for games in which a player was eligible by
    #   iterating over every eligible date for the player starting with their 
    #   first game and determine if they played or not
    list_play_status = []
    for date in player_schedule['ALL_date']:
        list_play_status.append(
                (date in player['QB_date']) or (date in player['RB_date']) or 
                (date in player['WR_date']) or (date in player['DEF_date']))
    player_schedule['play_status'] = list_play_status
    
    return player_schedule

def generate_full_schedules(league):
    '''
    Purpose: Inserts schedule data for all games that a player could have played
        into each player's elo data

    Inputs
    ------
        league : string
            Type of league to process player data for (i.e. 'nfl' or 'college')
    
    Outputs
    -------
        xxx_elo.json and xxx_elo.csv : JSON and CSV File
            File written to disk that contains elo data with full game arrays
            to include those games/dates in which a player doesn't participate
    '''        
    # determine which variables are array-based
    list_variables_to_fix = (
            ['QB_date', 'QB_opp', 'RB_date', 'RB_opp',
             'WR_date', 'WR_opp', 'DEF_date', 'DEF_opp'] 
            + ['position', 'position_std'])
    if league == 'college':
        list_variables_to_fix = (
                list_variables_to_fix + 
                ['QB_qbrElo', 'QB_ypgElo', 'QB_ypcElo', 'QB_ypaElo', 'QB_pctElo',
                 'QB_tdElo', 'QB_intElo', 'QB_eloC', 
                 'RB_ypgElo', 'RB_ypcElo', 'RB_yfsElo', 'RB_tdElo', 'RB_eloC',
                 'WR_recElo', 'WR_ypgElo', 'WR_ypcElo', 'WR_tdElo', 'WR_eloC',
                 'DEF_tacklesElo', 'DEF_tflElo', 'DEF_sackElo', 'DEF_intElo', 
                 'DEF_pbuElo', 'DEF_ffElo'])    
    else:
        list_variables_to_fix = (
                list_variables_to_fix + 
                ['QB_qbrElo', 'QB_ypgElo', 'QB_ypcElo', 'QB_ypaElo', 'QB_pctElo',
                 'QB_tdElo', 'QB_intElo', 'QB_eloC', 
                 'RB_ypgElo', 'RB_ypcElo', 'RB_yfsElo', 'RB_tdElo', 'RB_eloC',
                 'WR_recElo', 'WR_ypgElo', 'WR_ypcElo', 'WR_tgtElo', 'WR_yptElo', 
                 'WR_tdElo', 'WR_eloC', 
                 'DEF_tacklesElo', 'DEF_tflElo', 'DEF_sackElo', 'DEF_qbhElo', 
                 'DEF_intElo', 'DEF_pbuElo','DEF_ffElo'])
                             
    # create a dictionary of converter statements that will be used to handle
    #   the ingest for all variables with an array
    dict_converters = {}
    
    # associate the variables with their converter function
    for variable in list_variables_to_fix:
        dict_converters[variable] = lambda x: ast.literal_eval(
                x) if x not in ['nan',''] else ''
    
    # read in the elo data for all players in the specified league
    df_elo = pd.read_csv(f'data_elo/{league}_elo.csv',
                         converters = dict_converters)
    df_elo = df_elo.fillna('')
    
#    # write dataframe to disk as backup
#    df_elo.to_csv(f'data_elo/{league}_elo_backup.csv', index = False)
    
    # read in the schedule of all teams in the specified league for all years
    if league == 'college':
        list_schedules = sorted(glob.glob('data_team/schedules/*ncaa*.csv'))
    else:
        list_schedules = sorted(glob.glob('data_team/schedules/*nfl*.csv'))
        
    # read in the list of teams each player has played for and create a 
    #   a dictionary for the player with years as keys and teams they played
    #   for as the values
    if league == 'college':
#        df_teams_all = pd.read_csv(
#                'data_scraped/ncaa_player_meta/player_lookup_ncaa.csv',
#                usecols = ['id_ncaa', 'list_schools'])
        df_teams_all = pd.read_csv(
                'data_scraped/ncaa_player_meta/player_lookup_ncaa.csv')
        dict_teams_all = df_teams_all[['id_ncaa','list_schools']].set_index(
                'id_ncaa').T.to_dict(orient = 'index')['list_schools']
    else:
        df_teams_all = pd.read_csv(
                'data_scraped/nfl_player_meta/player_lookup_nfl.csv',
                usecols = ['id_nfl', 'list_teams'])
        dict_teams_all = df_teams_all.set_index('id_nfl').T.to_dict(
                orient = 'index')['list_teams']

    # create a dataframe containing the schedules for all teams across all years
    df_schedule = pd.DataFrame()
    for schedule in list_schedules:
        if len(df_schedule) == 0:
            df_schedule = pd.read_csv(schedule)
        else:
            df_schedule = df_schedule.append(pd.read_csv(schedule))
    
    # iterate over every player in the elo data and 3 new arrays:
    #   - all dates they could have played
    #   - opponent IDs for each of those dates
    #   - True/False flag if they played or not
    list_new_rows = []
    for index, player in tqdm.tqdm(df_elo.iterrows()):
        player_id = player['unique_id']
        
        # check for missing player_ids
        if pd.isna(player_id):
            continue
        
        # extract the list of teams a player belongs to in dictionary form
        dict_teams_player = extract_team_dict(
                player_id, dict_teams_all, df_teams_all)
        
        # convert scraped data into desired array format
        try:
            dict_player = create_schedule_array(
                    dict_teams_player, player, df_schedule)
        except:
            print(f'Error processing scheduling data for player: player_id')
            dict_player = {}
            dict_player['ALL_date'] = []
            dict_player['ALL_opp'] = []
            dict_player['play_status'] = []
        
        # add player elo data to newly created schedule arrays
        dict_player.update(player.to_dict())
        
        # convert data to row format for writing to disk
        list_new_rows.append(dict_player)
        
    # convert list of player dictionaries into a Pandas DataFrame
    df_final = pd.DataFrame(list_new_rows)
    
    # reorder columns
    if league == 'nfl':
        df_final = df_final[[
                'unique_id', 'position', 'position_std', 'To', 'last_year',
                'QB_last', 'QB_count', 'QB_qbrElo', 'QB_ypgElo', 'QB_ypcElo', 
                'QB_ypaElo', 'QB_pctElo', 'QB_tdElo', 'QB_intElo', 'QB_eloC', 
                'QB_date', 'QB_opp', 
                'RB_last', 'RB_count', 'RB_ypgElo',
                'RB_ypcElo', 'RB_yfsElo', 'RB_tdElo', 'RB_eloC', 'RB_date', 'RB_opp',
                'WR_last', 'WR_count', 'WR_recElo', 'WR_ypgElo', 'WR_ypcElo',
                'WR_tgtElo', 'WR_yptElo', 'WR_tdElo', 'WR_eloC', 'WR_date', 'WR_opp',
                'DEF_lastTackles', 'DEF_lastTFL', 'DEF_lastPBU', 'DEF_count',
                'DEF_tacklesElo', 'DEF_tflElo', 'DEF_sackElo', 'DEF_qbhElo',
                'DEF_intElo', 'DEF_pbuElo', 'DEF_ffElo', 'DEF_date', 'DEF_opp',
                'ALL_date', 'ALL_opp', 'play_status']]
    else:
        df_final = df_final[[
                'unique_id', 'position', 'position_std', 'To',
                'QB_last', 'QB_count', 'QB_qbrElo', 'QB_ypgElo', 'QB_ypcElo', 
                'QB_ypaElo', 'QB_pctElo', 'QB_tdElo', 'QB_intElo', 'QB_eloC', 
                'QB_date', 'QB_opp', 
                'RB_last', 'RB_count', 'RB_ypgElo',
                'RB_ypcElo', 'RB_yfsElo', 'RB_tdElo', 'RB_eloC', 'RB_date', 'RB_opp',
                'WR_last', 'WR_count', 'WR_recElo', 'WR_ypgElo', 'WR_ypcElo',
                'WR_tdElo', 'WR_eloC', 'WR_date', 'WR_opp',
                'DEF_lastTackles', 'DEF_lastTFL', 'DEF_lastPBU', 'DEF_count',
                'DEF_tacklesElo', 'DEF_tflElo', 'DEF_sackElo',
                'DEF_intElo', 'DEF_pbuElo', 'DEF_ffElo', 'DEF_date', 'DEF_opp',
                'ALL_date', 'ALL_opp', 'play_status']]        

    # fill in missing values with blanks rather than float NaNs
    df_final = df_final.fillna('')

    # write DataFrame to disk as a CSV
    df_final.to_csv(f'data_elo/{league}_elo.csv', index = False)
    
    # Convert the dataframe to a dictionary
    dict_final = df_final.to_dict('records')    
        
    # Write dictionary to a .json file
    with open(f'data_elo/{league}_elo.json', 'wt') as out:
            json.dump(dict_final, out)
    
    return  

def extract_team_dict(player_id, dict_teams_all, df_teams_all):
    '''
    Purpose: Create a dictionary that contains the year a player played as the
        keys and the team they played for as values. This information is
        extracted from a list of tuples.

    Inputs
    ------
        player_id : string
            ID of player
        dict_teams_all : dictionary
            Contains all year-team tuples for all players in the specified league
            with the list of tuples serving as the values and the player ids
            serving as the keys
        df_teams_all : Pandas DataFrame
            Contains all recorded player-metadata for players in the league
    
    Outputs
    -------
        dict_teams_player : dictioanry
            Contains a series of year-school key-value pairs for each year in a 
            player's career (i.e. year the player played and the school they 
            played for)
    '''   
    # extract list of tuples
    list_teams = ast.literal_eval(dict_teams_all[player_id])
    
    dict_teams_player = {}
    
    # if team is missing, attempt to generate missing information
    if list_teams == '' or list_teams == []:
        # find the player in the lookup table
        player_meta = df_teams_all[df_teams_all['id_ncaa'] == player_id].to_dict(
                orient = 'records')[0]
        # for each year in the player's career, associate their school with their years
        try:
            for year in range(int(player_meta['from_ncaa']),
                              int(player_meta['to_ncaa']) + 1):
                dict_teams_player[year] = player_meta['school']
        except:
            pass
    else:
        # convert list of tuples to dictionary
        for team_tuple in list_teams:
            dict_teams_player[team_tuple[0]] = team_tuple[1]
    
    return dict_teams_player

#=============================================================================
# Working Code
#==============================================================================
    
# Set the project working directory
#path_dir = pathlib.Path('/home/ejreidelbach/Projects/draft-analytics-api/src/data')
#os.chdir(path_dir)

#generate_full_schedules('nfl')
#generate_full_schedules('college')