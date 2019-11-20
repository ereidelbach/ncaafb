#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 20 11:04:03 2019

@author: ejreidelbach

:DESCRIPTION: Scrapes all team-level and player-level statistics for 
    games at the NFL level within a user-specified time period.

:REQUIRES:
    - Refer to the Package Import section of the script
   
:TODO:
"""
 
#==============================================================================
# Package Import
#==============================================================================
import ast
import csv
import glob
import numpy as np
import os  
import pandas as pd
import pathlib
import re
import requests
import string

from bs4 import BeautifulSoup

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
        name_standardized = row['Team']
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

def run_scraper(filename, headers, url, team = False):
    '''
    Purpose: Scrape data from a specified page and write it to a CSV file

    Inputs
    ------
        filename : string
            Name of the file to be written to disk
        headers : list of strings
            List of column headers found in the data
        url : string
            URL of the site being scraped
        team : Boolean
            True indicates data is being scraped for a Team and thus a column
            for storing a player-id should not be created.  A value of False
            indicates that player data is being scraped an the ID should be
            created.
    
    Outputs
    -------
        filename : csv file
            Contains team or player-based statistics written to disk in a .csv
            file format
    '''        
    # Each page only contains 100 entries before the next page has to be queried
    # To facilitate this paging, we create an offset to add to the end of the
    # URL which increments in steps of 100.
    offset = '&offset={}'
    
    # Set the initial offset
    offsetCount = 0
    
    # Set a stop count (i.e. the number of times we'll retry scraping a page
    #   before we accept the fact that we've hit the end)
    stop_count = 0
 
    #Open the CSV writer
    with open(filename, 'w') as f:
        # Initialize the CSV file writer
        wr = csv.writer(f)
        
        # Write the headers of the CSV
        wr.writerow(headers)
    
        while True:   
            # Notify the user when it reaches the potential end of the data
            try:
                page_response = requests.get(url + offset.format(offsetCount))
            except requests.exceptions.RequestException as e:
                print(e)
                print("Potentially finished")
                break
            
            # Parse the new request with BeautifulSoup
            page_content = BeautifulSoup(page_response.content, "html.parser")
            
            # Find the table of data on the requested page
            table = page_content.find("table", {'id' : 'results'})
        
            # If the page didn't load properly, stop the loop and try again
            try:
                output = [[td.text for td in row.find_all(
                        'td')] for row in table.select ('tr')]
                if output == [[], []]:
                    if stop_count < 5:
                        stop_count = stop_count + 1
                        print("Error occured or End Reached: Trying Again")
                        continue
                    else:
                        print("Suspect end has been reached...stopping")
                        break
            except:
                if stop_count < 5:
                    stop_count = stop_count + 1
                    print("Error occured or End Reached: Trying Again")
                    continue
                else:
                    print("Suspect end has been reached...stopping")
                    break
                
            # if team data is NOT being scraped add player-IDs
            if team == False:
                # obtain the player-ids for all players
                list_ids = []        
                for row in table.select('tr'):
                    if row.find('td') == None:
                        list_ids.append([])
                    else:
                        try: 
                            list_ids.append([row.find('td')['data-append-csv']])
                        except:
                            list_ids.append([''])  
            
                # merge with existing data
                merged_output = []
                for player, pid in zip(output, list_ids):
                    merged_output.append(player + pid)
            else:
                merged_output = output
        
            # Remove any blank rows that exist for formatting/readability reasons
            writeout = [rowout for rowout in merged_output if rowout]   
            
            # Write out the rows of data
            wr.writerows(writeout)
            
            # figure out the date of our last row for tracking purposes
            soup_header = table.find('thead')
            count = -1 # start at negative one to account for the index column
            for column in soup_header.find_all('th'):
                # check if the column is the date column
                try:
                    if 'date' in column['data-stat']:
                        break
                    # if not, and if not in an alternate header, increase the count
                    else:
                        if column['class'] != ['over_header', 'center']:
                            count = count + 1    
                except:
                    pass
            
            # Increase offset to go to the next page
            offsetCount += 100
            print('On week: %s, Offset: %i' % (writeout[-1][count], offsetCount))

    f.close()

    return

def scrape_player_offense(min_year, max_year):
    '''
    Purpose: Scrape individual offensive statistics for all players in the 
        specified date range (i.e. from 'min_year' to 'max_year')
        
        Note:  if min_year == max_year, only one season will be scraped

    Inputs
    ------
        min_year : int
            Year in which to begin scraping
        max_year : int
            Year in which to end scraping 
    
    Outputs
    -------
        nfl_player_2xxx_off_pass.csv : csv file
            Contains data for all players with passing statistics
        nfl_player_2xxx_off_run.csv : csv file
            Contains data for all players with rushing statistics
        nfl_player_2xxx_off_rec.csv : csv file
            Contains data for all players with receiving statistics
    '''
    #---------------------- PASSING -------------------------------------------    
    # set the desired url for scraping passing statistics
    url_pass = ('https://www.pro-football-reference.com/play-index/pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR&pos%5B%5D=RB&pos%5B%5D=TE&pos%5B%5D=OL&pos%5B%5D=DL&pos%5B%5D=LB&pos%5B%5D=DB&is_starter=E&game_type=E&career_game_num_min=1&career_game_num_max=999&qb_start_num_min=1&qb_start_num_max=999&game_num_min=0&game_num_max=99&week_num_min=0&week_num_max=99&c1stat=pass_att&c1comp=gt&c1val=1&c2val=0&c3val=0&c4val=0&c5val=0&order_by=game_date&order_by_asc=Y'
            ).format(min_year, max_year)
            
    # rename the column headers to match the desired format
    headers_pass = ['Player', 'Pos', 'Age', 'Date', 'LG', 'Team', 'Loc', 
                    'Opponent', 'WL', 'G', 'Week', 'Day', 'Pass Comp', 
                    'Pass Att', 'Pass Pct', 'Pass Yard', 'Pass TD', 'Pass Int', 
                    'QBR', 'Sk', 'Sk Yds', 'YPA', 'AYPA',  'unique_id']

    # set the filename
    filename_pass = ''
    if min_year == max_year:
        filename_pass = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_off_pass.csv' % 
                (min_year))
    else:
        filename_pass = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_to_%i_off_pass.csv' % 
                (min_year, max_year))

    # scrape data and write to disk for offensive stats (passing)
    print('###------- Starting Scraper for NFL Offense (Passing) -------###')
    run_scraper(filename_pass, headers_pass, url_pass)

    #---------------------- RUSHING -------------------------------------------    
    # set the desired url for scraping rushing statistics
    url_run = ('https://www.pro-football-reference.com/play-index/pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR&pos%5B%5D=RB&pos%5B%5D=TE&pos%5B%5D=OL&pos%5B%5D=DL&pos%5B%5D=LB&pos%5B%5D=DB&is_starter=E&game_type=E&career_game_num_min=1&career_game_num_max=999&qb_start_num_min=1&qb_start_num_max=999&game_num_min=0&game_num_max=99&week_num_min=0&week_num_max=99&c1stat=rush_att&c1comp=gt&c1val=1&c2val=0&c3val=0&c4val=0&c5val=0&order_by=game_date&order_by_asc=Y'
            ).format(min_year, max_year)
      
    # rename the column headers to match the desired format
    headers_run = [
            'Player', 'Pos', 'Age', 'Date', 'LG', 'Team', 'Loc', 'Opponent', 
            'WL', 'G', 'Week', 'Day', 'Rush Att', 'Rush Yard', 'Rush Avg', 
            'Rush TD', 'unique_id']

    # set the filename
    filename_run = ''
    if min_year == max_year:
        filename_run = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_off_run.csv' % 
                (min_year))
    else:
        filename_run = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_to_%i_off_run.csv' % 
                (min_year, max_year))

    # scrape data and write to disk for offensive stats (rushing)
    print('###------- Starting Scraper for NFL Offense (Rushing) -------###')
    run_scraper(filename_run, headers_run, url_run)
    
    #---------------------- RECEIVING------------------------------------------    
    # set the desired url for scraping receiving statistics
    url_rec = ('https://www.pro-football-reference.com/play-index/pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR&pos%5B%5D=RB&pos%5B%5D=TE&pos%5B%5D=OL&pos%5B%5D=DL&pos%5B%5D=LB&pos%5B%5D=DB&is_starter=E&game_type=E&career_game_num_min=1&career_game_num_max=999&qb_start_num_min=1&qb_start_num_max=999&game_num_min=0&game_num_max=99&week_num_min=0&week_num_max=99&c1stat=rec&c1comp=gt&c1val=1&c2val=0&c3val=0&c4val=0&c5val=0&order_by=game_date&order_by_asc=Y'
            ).format(min_year, max_year)
      
    # rename the column headers to match the desired format
    headers_rec = [
            'Player', 'Pos', 'Age', 'Date', 'LG', 'Team', 'Loc', 'Opponent', 
            'WL', 'G', 'Week', 'Day', 'Targets', 'Rec', 'Rec Yards', 'Rec Avg', 
            'Rec TD', 'Catch Pct', 'YPTgt', 'unique_id']

    # set the filename
    filename_rec = ''
    if min_year == max_year:
        filename_rec = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_off_rec.csv' % 
                (min_year))
    else:
        filename_rec = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_to_%i_off_rec.csv' % 
                (min_year, max_year))

    # scrape data and write to disk for offensive stats (receiving)
    print('###------- Starting Scraper for NFL Offense (Receiving) -------###')
    run_scraper(filename_rec, headers_rec, url_rec)

#    #---------------------- FUMBLES -------------------------------------------    
#    # set the desired url for scraping fumble statistics
#    url_fumb = (
#            'https://www.pro-football-reference.com/play-index/' + 
#            'pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&' + 
#            'season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR&' + 
#            'pos%5B%5D=RB&pos%5B%5D=TE&is_starter=E&game_type=E&career_' + 
#            'game_num_min=1&career_game_num_max=999&qb_start_num_min=1&' + 
#            'qb_start_num_max=999&game_num_min=0&game_num_max=99&week_' + 
#            'num_min=0&week_num_max=99&c1stat=fumbles&c1comp=gt&c1val=1&' + 
#            'c2val=0&c3val=0&c4val=0&c5val=0&order_by=game_date&order_by_asc=Y'
#            ).format(min_year, max_year)
#      
#    # rename the column headers to match the desired format
#    headers_fumb = [
#            'Player', 'Pos', 'Age', 'Date', 'LG', 'Team', 'Loc', 'Opponent', 
#            'WL', 'G', 'Week', 'Day', 'Fumbles', 'Forced Fumbles', 
#            'Fumbles Recovered', 'Fumble Ret Yds', 'Fumble Ret TD', 'unique_id']
#
#    # set the filename
#    filename_fumb = ''
#    if min_year == max_year:
#        filename_fumb = (
#                'data_scraped/nfl_player/sub_stats/nfl_player_%i_off_fumb.csv' % 
#                (min_year))
#    else:
#        filename_fumb = (
#                'data_scraped/nfl_player/sub_stats/nfl_player_%i_to_%i_off_fumb.csv' % 
#                (min_year, max_year))
#
#    # scrape data and write to disk for offensive stats
#    run_scraper(filename_fumb, headers_fumb, url_fumb)
        
    return

def scrape_player_defense(min_year, max_year):
    '''
    Purpose: Scrape individual statistics for all defensive players in the 
        specified date range (i.e. from 'min_year' to 'max_year')
        
        Note 1:  if min_year == max_year, only one season will be scraped            

    Inputs
    ------
        min_year : int
            Year in which to begin scraping
        max_year : int
            Year in which to end scraping 
    
    Outputs
    -------
        nfl_player_2xxx_def_tackles.csv : csv file
            Contains player-based statistics for every defensive player on 
            every team across all games played concerning combined tackles
        nfl_player_2xxx_def_int.csv : csv file
            Contains player-based statistics for every defensive player on 
            every team across all games played concerning passes defended
        nfl_player_2xxx_def_fum.csv : csv file
            Contains player-based statistics for every defensive player on 
            every team across all games played concerning forced fumbles
    '''            
    #---------------------- TACKLES - COMBINED --------------------------------
    url_tackles = ('https://www.pro-football-reference.com/play-index/pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR&pos%5B%5D=RB&pos%5B%5D=TE&pos%5B%5D=OL&pos%5B%5D=DL&pos%5B%5D=LB&pos%5B%5D=DB&is_starter=E&game_type=E&career_game_num_min=1&career_game_num_max=999&qb_start_num_min=1&qb_start_num_max=999&game_num_min=0&game_num_max=99&week_num_min=0&week_num_max=99&c1stat=tackles_combined&c1comp=gt&c1val=1&c2val=0&c3val=0&c4val=0&c5val=0&order_by=game_date&order_by_asc=Y'
               ).format(min_year, max_year)
    
    # Rename various column headers
    headers_tackles = [
            'Player', 'Pos', 'Age', 'Date', 'LG', 'Team', 'Loc', 'Opponent', 
            'WL', 'G', 'Week', 'Day', 'Sack', 'Tackle Solo', 'Tackle Assist', 
            'Tackle Tot', 'Tackle for Loss', 'QBHits', 'unique_id']
    
    # set the filename
    filename_tackles = ''
    if min_year == max_year:
        filename_tackles = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_def_tackles.csv' 
                % (min_year))
    else:
        filename_tackles = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_to_%i_def_tackles.csv' % 
                (min_year, max_year))

    # scrape data and write to disk for tackle stats
    print('###------- Starting Scraper for NFL Defensive (Tackles) -------###')
    run_scraper(filename_tackles, headers_tackles, url_tackles)

    #---------------------- PASSES DEFENDED------------------------------------
    url_int = ('https://www.pro-football-reference.com/play-index/pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR&pos%5B%5D=RB&pos%5B%5D=TE&pos%5B%5D=OL&pos%5B%5D=DL&pos%5B%5D=LB&pos%5B%5D=DB&is_starter=E&game_type=E&career_game_num_min=1&career_game_num_max=999&qb_start_num_min=1&qb_start_num_max=999&game_num_min=0&game_num_max=99&week_num_min=0&week_num_max=99&c1stat=pass_defended&c1comp=gt&c1val=1&c2val=0&c3val=0&c4val=0&c5val=0&order_by=game_date&order_by_asc=Y'
               ).format(min_year, max_year)
    
    # Rename various column headers
    headers_int = [
            'Player', 'Pos', 'Age', 'Date', 'LG', 'Team', 'Loc', 'Opponent', 
            'WL', 'G', 'Week', 'Day', 'Int Ret', 'Int Ret Yds', 'Int Ret TD', 
            'Pass Broken Up', 'unique_id']
    
    # set the filename
    filename_int = ''
    if min_year == max_year:
        filename_int = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_def_int.csv' 
                % (min_year))
    else:
        filename_int = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_to_%i_def_int.csv' % 
                (min_year, max_year))

    # scrape data and write to disk for interceptions / passes defended stats
    print('###------- Starting Scraper for NFL Defensive (Interceptions) -------###')
    run_scraper(filename_int, headers_int, url_int)


    #---------------------- FUMBLES FORCED ------------------------------------
    url_fum = ('https://www.pro-football-reference.com/play-index/pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR&pos%5B%5D=RB&pos%5B%5D=TE&pos%5B%5D=OL&pos%5B%5D=DL&pos%5B%5D=LB&pos%5B%5D=DB&is_starter=E&game_type=E&career_game_num_min=1&career_game_num_max=999&qb_start_num_min=1&qb_start_num_max=999&game_num_min=0&game_num_max=99&week_num_min=0&week_num_max=99&c1stat=fumbles_forced&c1comp=gt&c1val=1&c2val=0&c3val=0&c4val=0&c5val=0&order_by=game_date&order_by_asc=Y'
               ).format(min_year, max_year, max_year)
    
    # Rename various column headers
    headers_fum = [
            'Player', 'Pos', 'Age', 'Date', 'LG', 'Team', 'Loc', 'Opponent', 
            'WL', 'G', 'Week', 'Day',  'Fmb', 'Fumble Forced', 'FR', 'FR Yds', 
            'FR TD', 'unique_id']
    
    # set the filename
    filename_fum = ''
    if min_year == max_year:
        filename_fum = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_def_fum.csv' 
                % (min_year))
    else:
        filename_fum = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_to_%i_def_fum.csv' % 
                (min_year, max_year))

    # scrape data and write to disk for forced fumble stats
    print('###------- Starting Scraper for NFL Defensive (Fumbles) -------###')
    run_scraper(filename_fum, headers_fum, url_fum)
    return

def scrape_player_kicking(min_year, max_year):
    '''
    Purpose: Scrape individual statistics for all kicking related info in the 
        specified date range (i.e. from 'min_year' to 'max_year')
        
        Note 1:  if min_year == max_year, only one season will be scraped

    Inputs
    ------
        min_year : int
            Year in which to begin scraping
        max_year : int
            Year in which to end scraping 
    
    Outputs
    -------
        nfl_player_2xxx_kick_xp.csv : csv file
            Contains statistics for every player who returned kicked extra 
            points across all games played for the specified time window.
        nfl_player_2xxx_kick_fg.csv : csv file
            Contains statistics for every player who returned kicked field 
            goals across all games played for the specified time window.
    '''            
    #---------------------- EXTRA POINTS---------------------------------------
    # URL for scraping extra point data (kickers part 1)
    url_xp = ('https://www.pro-football-reference.com/play-index/' + 
                   'pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}' + 
                   '&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR' + 
                   '&pos%5B%5D=RB&pos%5B%5D=TE&pos%5B%5D=OL&pos%5B%5D=DL' + 
                   '&pos%5B%5D=LB&pos%5B%5D=DB&is_starter=E&game_type=E' + 
                   '&career_game_num_min=1&career_game_num_max=999' + 
                   '&qb_start_num_min=1&qb_start_num_max=999&game_num_min=0' + 
                   '&game_num_max=99&week_num_min=0&week_num_max=99' + 
                   '&c1stat=xpa&c1comp=gt&c1val=1&c5val=0' + 
                   '&order_by=game_date&order_by_asc=Y'
                   ).format(min_year, max_year, max_year)

    # Rename various column headers
    headers_xp = [
            'Player', 'Pos', 'Age', 'Date', 'LG', 'Team', 'Loc', 'Opponent', 
            'WL', 'G', 'Week', 'Day', 'XP Made', 'XP Att', 'XP %', 'FG Made', 
            'FG Att', 'FG %', '2PM', 'Sfty', 'TD', 'Pts', 'unique_id']
    
    # set the filename
    filename_xp = ''
    if min_year == max_year:
        filename_xp = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_kick_xp.csv' 
                % (min_year))
    else:
        filename_xp = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_to_%i_kick_xp.csv' %
                (min_year, max_year))

    # scrape data and write to disk for field goal stats
    print('###------- Starting Scraper for NFL Kicking (Extra Points) -------###')
    run_scraper(filename_xp, headers_xp, url_xp)
    
    #---------------------- FIELD GOALS ---------------------------------------
    # URL for scraping extra point data (kickers part 2)
    url_fg = ('https://www.pro-football-reference.com/play-index/' + 
                   'pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}' + 
                   '&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR' + 
                   '&pos%5B%5D=RB&pos%5B%5D=TE&pos%5B%5D=OL&pos%5B%5D=DL' + 
                   '&pos%5B%5D=LB&pos%5B%5D=DB&is_starter=E&game_type=E' + 
                   '&career_game_num_min=1&career_game_num_max=999' + 
                   '&qb_start_num_min=1&qb_start_num_max=999&game_num_min=0' + 
                   '&game_num_max=99&week_num_min=0&week_num_max=99' + 
                   '&c1stat=fga&c1comp=gt&c1val=1&c5val=0' + 
                   '&order_by=game_date&order_by_asc=Y'
                   ).format(min_year, max_year, max_year)

    # Rename various column headers
    headers_fg = [
            'Player', 'Pos', 'Age', 'Date', 'LG', 'Team', 'Loc', 'Opponent', 
            'WL', 'G', 'Week', 'Day', 'XP Made', 'XP Att', 'XP %', 'FG Made', 
            'FG Att', 'FG %', '2PM', 'Sfty', 'TD', 'Pts', 'unique_id']
    
    # set the filename
    filename_fg = ''
    if min_year == max_year:
        filename_fg = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_kick_fg.csv' 
                % (min_year))
    else:
        filename_fg = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_to_%i_kick_fg.csv' %
                (min_year, max_year))

    # scrape data and write to disk for extra point stats
    print('###------- Starting Scraper for NFL Kicking (Field Goals) -------###')
    run_scraper(filename_fg, headers_fg, url_fg)
    
    return

def scrape_player_returning(min_year, max_year):
    '''
    Purpose: Scrape individual statistics for all return-related info in the 
        specified date range (i.e. from 'min_year' to 'max_year')
        
        Note 1:  if min_year == max_year, only one season will be scraped            

    Inputs
    ------
        min_year : int
            Year in which to begin scraping
        max_year : int
            Year in which to end scraping 
    
    Outputs
    -------
        nfl_player_2xxx_ret_punt.csv : csv file
            Contains statistics for every player who returned punts across all 
            games played for the specified time window.
        nfl_player_2xxx_ret_kickoff.csv : csv file
            Contains statistics for every player who returned kickoffs across 
            all games played for the specified time window.
    '''    
    #---------------------- KICK RETURNS --------------------------------------     
    # URL for scraping kick return data (special teams part 1)
    url_kick_ret = ('https://www.pro-football-reference.com/play-index/' + 
                    'pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}' + 
                    '&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR&' + 
                    'pos%5B%5D=RB&pos%5B%5D=TE&pos%5B%5D=OL&pos%5B%5D=DL&' + 
                    'pos%5B%5D=LB&pos%5B%5D=DB&is_starter=E&game_type=E&' + 
                    'career_game_num_min=1&career_game_num_max=999&' + 
                    'qb_start_num_min=1&qb_start_num_max=999&game_num_min=0&' + 
                    'game_num_max=99&week_num_min=0&week_num_max=99&' + 
                    'c1stat=kick_ret&c1comp=gt&c1val=1&c5val=0' + 
                    '&order_by=game_date&order_by_asc=Y'
                    ).format(min_year, max_year, max_year)

    # Rename various column headers
    headers_kick_ret = [
            'Player', 'Pos', 'Age', 'Date', 'LG', 'Team', 'Loc', 'Opponent', 
            'WL', 'G', 'Week', 'Day', 'Kick Ret', 'Kick Ret Yds', 'Kick Ret YPA',
            'Kick Ret TD', 'unique_id']
    
    # set the filename
    filename_kick_ret = ''
    if min_year == max_year:
        filename_kick_ret = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_ret_kick.csv' 
                % (min_year))
    else:
        filename_kick_ret = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_to_%i_ret_kick.csv' %
                (min_year, max_year))

    # scrape data and write to disk for kick return stats
    print('###------- Starting Scraper for NFL Returns (Kickoffs) -------###')
    run_scraper(filename_kick_ret, headers_kick_ret, url_kick_ret)    
    
    #---------------------- PUNT RETURNS --------------------------------------
    # URL for scraping punt return data (special teams part 1)
    url_punt_ret = ('https://www.pro-football-reference.com/play-index/' + 
                    'pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}' + 
                    '&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR&' + 
                    'pos%5B%5D=RB&pos%5B%5D=TE&pos%5B%5D=OL&pos%5B%5D=DL&' + 
                    'pos%5B%5D=LB&pos%5B%5D=DB&is_starter=E&game_type=E&' + 
                    'career_game_num_min=1&career_game_num_max=999&' + 
                    'qb_start_num_min=1&qb_start_num_max=999&game_num_min=0&' + 
                    'game_num_max=99&week_num_min=0&week_num_max=99&' + 
                    'c1stat=punt_ret&c1comp=gt&c1val=1&c5val=0' + 
                    '&order_by=game_date&order_by_asc=Y'
                    ).format(min_year, max_year, max_year)
    
    # Rename various column headers
    headers_punt_ret = [
            'Player', 'Pos', 'Age', 'Date', 'LG', 'Team', 'Loc', 'Opponent', 
            'WL', 'G', 'Week', 'Day', 'Punt Ret', 'Punt Ret Yds', 'Punt Ret YPA',
            'Punt Ret TD', 'unique_id']
    
    # set the filename
    filename_punt_ret = ''
    if min_year == max_year:
        filename_punt_ret = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_ret_punt.csv' 
                % (min_year))
    else:
        filename_punt_ret = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_to_%i_ret_punt.csv' % 
                (min_year, max_year))

    # scrape data and write to disk for punt return stats
    print('###------- Starting Scraper for NFL Returns (Punts) -------###')
    run_scraper(filename_punt_ret, headers_punt_ret, url_punt_ret)
    
    return

def scrape_player_fantasy(min_year, max_year):
    '''
    Purpose: Scrape individual statistics for all fantasy-related info in the 
        specified date range (i.e. from 'min_year' to 'max_year')
        
        Note 1:  if min_year == max_year, only one season will be scraped            

    Inputs
    ------
        min_year : int
            Year in which to begin scraping
        max_year : int
            Year in which to end scraping 
    
    Outputs
    -------
        nfl_player_2xxx_fantasy.csv : csv file
            Contains statistics for every player who earned fantasy points 
            across all games played for the specified time window.
    '''         
    # URL for scraping fantasy data (special teams part 1)
    url_fantasy = ('https://www.pro-football-reference.com/play-index/' + 
                   'pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}' + 
                   '&season_start=1&season_end=-1&pos%5B%5D=QB&pos%5B%5D=WR' + 
                   '&pos%5B%5D=RB&pos%5B%5D=TE&pos%5B%5D=OL&pos%5B%5D=DL&' + 
                   'pos%5B%5D=LB&pos%5B%5D=DBis_starter=E&game_type=E&' + 
                   'career_game_num_min=1&career_game_num_max=999&qb_start_' + 
                   'num_min=1&qb_start_num_max=999&game_num_min=0&game_num_' + 
                   'max=99&week_num_min=0&week_num_max=99&c1stat=fantasy_' + 
                   'points&c1comp=gt&c1val=1&c2val=0&c3val=0&c4val=0&c5val=0' + 
                   '&order_by=game_date&order_by_asc=Y'
                   ).format(min_year, max_year, max_year)

    # Rename various column headers
    headers_fantasy = [
            'Player', 'Pos', 'Age', 'Date', 'LG', 'Team', 'Loc', 'Opponent', 
            'WL', 'G', 'Week', 'Day', 'Fantasy Pts', 'PPR', 'DK Pts', 'FD Pts', 
            'Pass_cmp', 'Pass_att', 'Pass_yds', 'Pass_td', 'Pass_int', 
            'Run_att', 'Run_yds', 'Run_td', 'Rec', 'Rec_yds', 'Rec_td', 'Fmb', 
            'FGM', 'FGA', 'XPM', 'XPA', 'unique_id']
    
    # set the filename
    filename_fantasy = ''
    if min_year == max_year:
        filename_fantasy = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_fantasy.csv' 
                % (min_year))
    else:
        filename_fantasy = (
                'data_scraped/nfl_player/sub_stats/nfl_player_%i_to_%i_fantasy.csv' % 
                (min_year, max_year))

    # scrape data and write to disk for fantasy stats
    print('###------- Starting Scraper for NFL Fantasy Stats -------###')
    run_scraper(filename_fantasy, headers_fantasy, url_fantasy)    
    
    return

def scrape_team_statistics(min_year, max_year):
    '''
    Purpose: Scrape game statistics for all teams in the specified date range
        (i.e. from 'min_year' to 'max_year')
        
        Note:  if min_year == max_year, only one season will be scraped

    Inputs
    ------
        min_year : int
            Year in which to begin scraping
        max_year : int
            Year in which to end scraping 
    
    Outputs
    -------
        nfl_team_data_2xxx.csv : csv file
            Contains team-based statistics for every team across all games 
            played (specifically passing and rushing statistics). For every
            game, there will be 2 rows (1 for each team) unless one of the
            teams is not FBS (in which case their will be no row for that team)
    '''    
    # set the desired url for scraping purposes using the input date range
    url = ('https://www.pro-football-reference.com/play-index/' + 
                'tgl_finder.cgi?request=1&match=game&year_min={}&year_max={}' + 
                '&game_type=E&game_num_min=0&game_num_max=99&week_num_min=0&' + 
                'week_num_max=99&temperature_gtlt=lt&c1stat=points&c1comp=gte' + 
                '&c1val=0&c2stat=pass_att&c2comp=gte&c2val=0&c3stat=rush_att' + 
                '&c3comp=gte&c3val=0&c4stat=pass_att_opp&c4comp=gte&c4val=0' + 
                '&c5stat=rush_att_opp&c5comp=gte&c5val=1.0&c6stat=rush_att_opp' + 
                '&order_by=game_date&order_by_asc=Y'
                ).format(min_year, max_year)
    
    # Rename various column headers
    headers = ['Team', 'Year', 'Date', 'Time', 'LTime', 'Loc', 'Opponent', 
               'Week', 'G', 'Day', 'Result', 'OT', 'Points For', 
               'Points Against', 'Points Difference', 'Points Combined',               
               'Pass Comp', 'Pass Att', 
               'Comp Pct', 'Pass Yard', 'Pass TD', 'Pass Int', 'Sk', 'Yds', 
               'Pass Rate', 'Rush Att', 'Rush Yard', 'Rush Avg', 'Rush TD',
               'Pass Comp opp', 'Pass Att opp', 'Comp Pct opp', 'Pass Yard opp',
               'Pass TD opp', 'Pass Int opp', 'Sk opp', 'Yds opp', 
               'Pass Rate opp', 'Rush Att opp', 'Rush Yard opp', 'Rush Avg opp',
               'Rush TD opp']
    
    # set the filename
    filename = ''
    if min_year == max_year:
        filename = 'data_scraped/nfl_team/nfl_team_data_%i.csv' % (min_year)
    else:
        filename = ('data_scraped/nfl_team/nfl_team_data_%i_to_%i.csv' % 
                    (min_year, max_year))
    
    # scrape data and write to disk
    print('###------- Starting Scraper for NFL Team Statistics -------###')
    run_scraper(filename, headers, url, team = True)
    
    # format the scraped team stats
    format_team_statistics(min_year)
    
    return

def set_team_code(list_teams,year):
    '''
    Purpose: Determine the NFL team code for each player's respective NFL team 
        (if they exist) and the code of their opponent

    Inputs
    ------
        list_teams : list of strings
            list of teams that require team codes
        year : int
            year for which data is being altered
                Note: The Cleveland Browns became the Baltimore Ravens at the
                    start of the 1996 season
                Note: The Houston Oilers became the Tennessee Titans at the 
                    start of the 1997 season

    Outputs
    -------
        list_codes : list of ints
            list of codes for each team
    '''
    # read in team dataframe
    df_teams = pd.read_csv('data_team/teams_nfl.csv')

    # convert the dataframe to a dictionary with keys as names and values as codes
    dict_teams = {}
    for index, row in df_teams.iterrows():
        # isolate the alternative name columns
        names = row[[x for x in row.index if 'Name' in x]]
        # convert the row to a list that doesn't include NaN values
        list_names = [x.strip() for x in names.values.tolist() if (
                (str(x) != 'nan') and (' ' not in str(x)))]
        # extract the standardized team name
        name_standardized = row['Team'].strip()
        # add the standardized name
        list_names.append(name_standardized)
        # extract the team code
        team_code = row['TeamCode']
        # for every alternative spelling of the team, set the value to be
        #   the standardized name
        for name_alternate in list_names:
            dict_teams[name_alternate] = team_code
            
    # if the year is 1996 or earlier, the Cleveland Browns should be assigned
    #   team code for the Ravens
    if year <= 1995:
        dict_teams['CLE'] = dict_teams['BAL']
        dict_teams['HOU'] = dict_teams['TEN']
    elif year == 1996:
        dict_teams['HOU'] = dict_teams['TEN']
        
    list_codes = []
    for team in list_teams:
        try:
            list_codes.append(dict_teams[team])
        except:
            list_codes.append(np.nan)
        
    return list_codes    

def merge_player_files_for_year(year):
    '''
    Purpose: When player statistical data is scraped, it results in multiple
        files for a given year. This function merges all those files into a 
        single dataset for the year based on player IDs

    Inputs
    ------
        year : int
            Year for which data is to be combined
    
    Outputs
    -------
        nfl_player_2xxx.csv : csv file
            Contains all player-based statistics for all scraped categories, 
            written to disk in a .csv file format
    ''' 
    # create the master dataframe
    df_master = pd.DataFrame()
    
    print('###------- MERGING FILES FOR %s -------###' % str(year))
    
    # iterate through all files and merge data together    
    for file in glob.glob(
            'data_scraped/nfl_player/sub_stats/*%s*.csv' % str(year)):
        # ignore any existing master file
        if file == (
                'data_scraped/nfl_player/sub_stats/nfl_player_%s.csv' % str(year)):
            continue
        
        # read in the file
        df_file = pd.read_csv(file)
        
        # for fantasy info, drop redundant statistical columns
        if 'fantasy' in file:
            df_file = df_file.drop(columns = [
                    'Pass_cmp', 'Pass_att', 'Pass_yds', 'Pass_td', 'Pass_int', 
                    'Run_att', 'Run_yds', 'Run_td', 'Rec', 'Rec_yds', 'Rec_td'])
        
        # for defensive forced fumbles, drop a redudant column ('Fumbles')
        if 'def_fum' in file:
            df_file = df_file.drop(columns = ['Fmb'])
        
        # if master is empty, set its contents to those in the current file
        if len(df_master) == 0:
            df_master = df_file.copy()
        # if the new file is empty (i.e. contains no data) skip it
        elif len(df_file) == 0:
            continue
        # otherwise, merge the file's contents into master
        else:
            df_master = pd.merge(
                    df_master,
                    df_file,
                    how = 'outer',
                    left_on = [
                            'Player', 'Pos', 'Age', 'Date', 'LG', 'Team', 'Loc', 
                            'Opponent', 'WL', 'G', 'Week', 'Day', 'unique_id'
                            ],
                    right_on = [
                            'Player', 'Pos', 'Age', 'Date', 'LG', 'Team', 'Loc', 
                            'Opponent', 'WL', 'G', 'Week', 'Day', 'unique_id'
                            ]
                    )          
            
            # check to make sure all player data is transferred over
            list_ids = df_master['unique_id'].tolist()
            for new_id in df_file['unique_id']:
                if new_id not in list_ids:
                    print('ID not found in master: %s' % new_id)

    # insert team codes for each team and their respective opponent
    df_master['Team Code'] = set_team_code(df_master['Team'].tolist(), year)
    df_master['Team Code opp'] = set_team_code(
            df_master['Opponent'].tolist(), year)    
    
    # standardize team names for the player's team
    df_master = rename_nfl(df_master, 'Team')
    # standardize team names for the player's opponent
    df_master = rename_nfl(df_master, 'Opponent')
    
    # remove any extra spaces from the player_id variable to prevent hanging spaces
    df_master['unique_id'] = df_master['unique_id'].apply(
            lambda x: x.strip() if type(x) == str else x)

    #----------------- Correct non-standardized player positions --------------
    # standardize position values
    position_std = []
    # for every position a player has, swap out the value for a std version
    for index, row in df_master.iterrows():
        pos = row['Pos']
        # handle NaN position values
        if pd.isna(pos):
            position_std.append('')
            continue
        # handle character-based position values
        else:
            # split the position variable (in case of multiple positions)
            list_pos = re.split('[-/]', pos)
            row_std = []
            for sub_pos in list_pos:
                if sub_pos in dict_positions.keys():
                    # extract the standardized value
                    pos_std = dict_positions[sub_pos]
                    # add the standardized position if it doesn't already exist
                    if pos_std not in row_std:
                        row_std.append(pos_std)
                # if a position exists that we don't handle, ignore it
                else:
                    row_std.append('')
                    print('Non-standard position value found for %s at row %i: %s'
                          % (row['unique_id'], index, sub_pos))
            position_std.append('/'.join(x for x in row_std if x != ''))
#                    print('Player %s messed up at row %i -- %s' % 
#                          (row['unique_id'], index, sub_pos))
    df_master['Pos'] = position_std

    # read in metadata
    df_meta = pd.read_csv('data_scraped/nfl_player_meta/player_lookup_nfl.csv')
    df_meta = df_meta[['id_nfl','pos_nfl_std']]
    df_meta = df_meta.rename(columns = {'id_nfl':'unique_id', 
                                        'pos_nfl_std':'Pos'})
        
    # merge metadata with the year's file
    df_merged = pd.merge(df_master,
                         df_meta,
                         how = 'left',
                         left_on = 'unique_id',
                         right_on = 'unique_id',
                         suffixes = ('_year', '_meta'))
    
    # fill all NaNs with blanks
    df_merged = df_merged.fillna('')
    
    # transition position data from list to string version separated by `/`
    list_positions = []
    for pos in zip(list(df_merged['Pos_year']), list(df_merged['Pos_meta'])):
        if pos[1] != '':
            pos = ast.literal_eval(pos[1])
            list_positions.append('/'.join(x for x in pos))
        else:
            list_positions.append(pos[0])
    
    # insert new position variable
    df_merged['Pos'] = list_positions
    
    # reorder columns (drop 'Pos_year' and 'Pos_meta')
    df_merged = df_merged[['Player', 'unique_id', 'Pos','Age', 
               'Date', 'LG', 'Team', 'Team Code', 'Loc', 'Opponent', 
               'Team Code opp', 'WL', 'G', 'Week', 'Day', 'Pass Comp', 
               'Pass Att', 'Pass Pct', 'Pass Yard', 'Pass TD', 'Pass Int', 
               'QBR', 'Sk', 'Sk Yds', 'YPA', 'AYPA', 'Rush Att', 'Rush Yard', 
               'Rush Avg', 'Rush TD', 'Targets', 'Rec', 'Rec Yards', 'Rec Avg', 
               'Rec TD', 'Catch Pct', 'YPTgt', 'Fmb', 'Tackle Solo', 
               'Tackle Assist', 'Tackle Tot', 'Tackle for Loss', 'QBHits', 
               'Sack', 'Int Ret', 'Int Ret Yds', 'Int Ret TD', 'Pass Broken Up', 
               'Fumble Forced', 'FR', 'FR Yds', 'FR TD', 'Kick Ret', 
               'Kick Ret Yds', 'Kick Ret YPA', 'Kick Ret TD', 'Punt Ret', 
               'Punt Ret Yds', 'Punt Ret YPA', 'Punt Ret TD', 'FGM', 'FGA', 
               'XPM', 'XPA', 'Fantasy Pts', 'PPR', 'DK Pts', 'FD Pts']]
    
    # remove HoF asterisks from player names
    df_merged['Player'] = df_master['Player'].apply(lambda x: x.replace('*',''))

    # output to disk        
    df_merged.to_csv('data_scraped/nfl_player/nfl_player_%i.csv' % (year), 
                     index = False)
        
    return

def format_team_statistics(year):
    '''
    Purpose: Read in and adjust formatting of scraped team statistics to match
        the desired output.

    Inputs
    ------
        year : int
            Year for which data is to be combined
    
    Outputs
    -------
        nfl_team_data_2xxx.csv : csv file
            Contains all player-based statistics for all scraped categories, 
            written to disk in a .csv file format
    ''' 
    filename = 'data_scraped/nfl_team/nfl_team_data_%s.csv' % str(year)
    df_year = pd.read_csv(filename)

    # insert team codes for each team and their respective opponent
    df_year['Team Code'] = set_team_code(df_year['Team'].tolist(), year)
    df_year['Team Code opp'] = set_team_code(df_year['Opponent'].tolist(), year)    
    
    # standardize team names for the player's team
    df_year = rename_nfl(df_year, 'Team')
    # standardize team names for the player's opponent
    df_year = rename_nfl(df_year, 'Opponent')
    
    # set desired order of columns
    df_year = df_year[[
            'Team', 'Team Code', 'Year', 'Date', 'Time', 'LTime', 'Loc',
            'Opponent', 'Team Code opp',
            'Week', 'G', 'Day', 'Result', 'OT', 'Points For', 'Points Against', 
            'Points Difference', 'Points Combined', 'Pass Comp', 'Pass Att', 
            'Comp Pct', 'Pass Yard', 'Pass TD', 'Pass Int', 'Sk', 'Yds', 
            'Pass Rate', 'Rush Att', 'Rush Yard', 'Rush Avg', 'Rush TD',
            'Pass Comp opp', 'Pass Att opp', 'Comp Pct opp', 'Pass Yard opp',
            'Pass TD opp', 'Pass Int opp', 'Sk opp', 'Yds opp', 
            'Pass Rate opp', 'Rush Att opp', 'Rush Yard opp', 'Rush Avg opp',
            'Rush TD opp']]
    
    # output file to disk
    df_year.to_csv('data_scraped/nfl_team/nfl_team_data_%s.csv' % 
                   str(year), index = False)
    
    return

def main():
    pass

#=============================================================================
# Working Code
#==============================================================================
if __name__ == "__main__":
    main()

## Set the project working directory
#path_dir = pathlib.Path('/home/ejreidelbach/Projects/draft-analytics-data/')
#os.chdir(path_dir)
#
### iterate over every year from 2005 to 2018 and scrape PLAYER data
#year = 2019
#scrape_player_offense(year, year)
#scrape_player_defense(year, year)
#scrape_player_kicking(year, year)
#scrape_player_returning(year, year)
#scrape_player_fantasy(year, year)
#merge_player_files_for_year(year)

## iterate over every year from 2005 to 2018 and scrape PLAYER data
#for year in range(1999,2020):
#    scrape_player_offense(year, year)
#    scrape_player_defense(year, year)
#    scrape_player_kicking(year, year)
#    scrape_player_returning(year, year)
#    scrape_player_fantasy(year, year)
#    merge_player_files_for_year(year)
#
## iterate over every year from 2000 to 2018 and scrape TEAM data
#for year in range(1994,2020):
#    scrape_team_statistics(year, year)
#    format_team_statistics(year)
    
    
#list_positions = []
#for year in range(1999, 2019):
#    df = pd.read_csv('data_scraped/nfl_player/nfl_player_%s.csv' % str(year))
#    for pos in df['Pos'].drop_duplicates().tolist():
#        if pos not in list_positions:
#            list_positions.append(pos)
#list_positions.remove(np.nan)
#list_positions = sorted(list_positions)
    
#def clean_up_positions(list_positions):
#    '''
#    Purpose: Pro-football reference position data within individual player
#        statistics game data is not always clean/properly formatted. This 
#        function will iterate through position data and deal with any potential
#        formatting issues.
#
#    Inputs
#    ------
#        list_positions : list of strings
#            list of positional data to be cleaned
#    
#    Outputs
#    -------
#        list_corrections : list of strings
#            list of properly formatted positional data
#    '''
#    list_corrections = []
#    
#    list_alpha = []
#    dict_alpha = {}
#    for pos in list_positions:
#        # process strings with only alphabetic characters
#        if pos.isalpha():
#            
#            
#        # process strings containing any non-alphabetic characters
#        else:
#            
#            pos_clean = ''
#            
#            # remove any numbers or `'` from the position variable
#            for char in pos:
#                if char in string.digits:
#                    pass
#                elif (char == "'") or (char == "`") or (char == "#"):
#                    pass
#                else:
#                    pos_clean = pos_clean + char
#                    
#            # handle arrays
#            if ' ' in pos.strip():
#                pos_clean = pos.split(' ')[0]
#                
#            # handle multiple positions
#                        
#        list_corrections.append(pos_clean.strip())
#    
#    for pos in list_corrections:
#        
#    
#    return list_corrections
#    
