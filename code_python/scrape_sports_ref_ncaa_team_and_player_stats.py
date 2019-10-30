#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  1 16:23:55 2019

@author: ejreidelbach

:DESCRIPTION: Scrapes all team-level and player-level statistics for games at 
    the NCAA (FBS) level within a user-specified time period.

:REQUIRES:
    - Refer to the Package Import section of the script
   
:TODO:
"""
 
#==============================================================================
# Package Import
#==============================================================================
import csv
import glob
import numpy as np
import os  
import pandas as pd
import pathlib
import requests

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
    df_school_names = pd.read_csv('Data/teams_ncaa.csv')  
     
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
            
            # Increase offset to go to the next page
            offsetCount += 100
            print('File: %s, On week: %s, Offset: %i' % (
                    filename.split('/')[-1].split('.csv')[0],
                    writeout[-1][2], offsetCount))

    f.close()

    return

def scrape_player_statistics_offense(min_year, max_year):
    '''
    Purpose: Scrape individual statistics for all players in the specified date 
        range (i.e. from 'min_year' to 'max_year')
        
        Note:  if min_year == max_year, only one season will be scraped

    Inputs
    ------
        min_year : int
            Year in which to begin scraping
        max_year : int
            Year in which to end scraping 
    
    Outputs
    -------
        ncaa_player_2xxx_pass.csv : csv file
            Contains player-based statistics for every player on every team
            across all games played (e.g. passing, rushing and defensive
            statistics). Statistics are only available for players on FBS teams.
    '''
    #---------------------- PASSING -------------------------------------------    
    # set the desired url for scraping passing statistics
    url_pass = (
            'https://www.sports-reference.com/cfb/play-index/pgl_finder.cgi?' + 
            'request=1&match=game&year_min={}&year_max={}&c1stat=pass_att' + 
            '&c1comp=gt&c1val=1&c2val=0&c3val=0&order_by=date_game&order_by_asc=Y'
            ).format(min_year, max_year)
            
    # rename the column headers to match the desired format
    headers_pass = ['Player', 'Date', 'G', 'School', 'Loc', 'Opponent', 'WL',
                   'Pass Comp', 'Pass Att', 'Pass Pct', 'Pass Yard', 'Pass TD', 
                   'Pass Int', 'QBR', 'unique_id']

    # set the filename
    filename_pass = ''
    if min_year == max_year:
        filename_pass = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_off_pass.csv' % 
                (min_year))
    else:
        filename_pass = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_to_%i_off_pass.csv' % 
                (min_year, max_year))

    # scrape data and write to disk for offensive stats
    print('###------- Starting Scraper for NCAA Offense (Passing) -------###')
    run_scraper(filename_pass, headers_pass, url_pass)

    #---------------------- RUSHING -------------------------------------------    
    # set the desired url for scraping rushing statistics
    url_run = (
            'https://www.sports-reference.com/cfb/play-index/pgl_finder.cgi?' + 
            'request=1&match=game&year_min={}&year_max={}&c1stat=rush_att' + 
            '&c1comp=gt&c1val=1&c2val=0&c3val=0&order_by=date_game&order_by_asc=Y'
            ).format(min_year, max_year)
      
    # rename the column headers to match the desired format
    headers_run = ['Player', 'Date', 'G', 'School', 'Loc', 'Opponent', 'WL',
                   'Rush Att', 'Rush Yard', 'Rush Avg', 'Rush TD', 'unique_id']

    # set the filename
    filename_run = ''
    if min_year == max_year:
        filename_run = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_off_run.csv' % 
                (min_year))
    else:
        filename_run = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_to_%i_off_run.csv' % 
                (min_year, max_year))

    # scrape data and write to disk for offensive stats
    print('###------- Starting Scraper for NCAA Offense (Rushing) -------###')
    run_scraper(filename_run, headers_run, url_run)
    
    #---------------------- RECEIVING------------------------------------------    
    # set the desired url for scraping receiving statistics
    url_rec = (
            'https://www.sports-reference.com/cfb/play-index/pgl_finder.cgi?' + 
            'request=1&match=game&year_min={}&year_max={}&c1stat=rec' + 
            '&c1comp=gt&c1val=1&c2val=0&c3val=0&order_by=date_game&order_by_asc=Y'
            ).format(min_year, max_year)
      
    # rename the column headers to match the desired format
    headers_rec = ['Player', 'Date', 'G', 'School', 'Loc', 'Opponent', 'WL',
                   'Rec', 'Rec Yards', 'Rec Avg', 'Rec TD', 'unique_id']

    # set the filename
    filename_rec = ''
    if min_year == max_year:
        filename_rec = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_off_rec.csv' % 
                (min_year))
    else:
        filename_rec = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_to_%i_off_rec.csv' % 
                (min_year, max_year))

    # scrape data and write to disk for offensive stats
    print('###------- Starting Scraper for NCAA Offense (Receiving) -------###')
    run_scraper(filename_rec, headers_rec, url_rec)
    
    return

def scrape_player_statistics_defense(min_year, max_year):
    '''
    Purpose: Scrape individual statistics for all players in the specified date 
        range (i.e. from 'min_year' to 'max_year')
        
        Note:  if min_year == max_year, only one season will be scraped

    Inputs
    ------
        min_year : int
            Year in which to begin scraping
        max_year : int
            Year in which to end scraping 
    
    Outputs
    -------
        ncaa_player_2xxx_def_tackles.csv : csv file
            Contains player-based statistics for every defensive player on 
            every team across all games played concerning combined tackles
        ncaa_player_2xxx_def_int.csv : csv file
            Contains player-based statistics for every defensive player on 
            every team across all games played concerning passes defended
        ncaa_player_2xxx_def_fum.csv : csv file
            Contains player-based statistics for every defensive player on 
            every team across all games played concerning forced fumbles
    '''
    #---------------------- TACKLES -------------------------------------------   
    # set the desired url for scraping tackles= statistics
    url_def_tackles = ('https://www.sports-reference.com/cfb/play-index/' + 
                       'pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&' + 
                       'c1stat=tackles_total&c1comp=gt&c1val=1&c2val=0&c3val=0' + 
                       '&c4val=0&order_by=date_game&order_by_asc=Y'
            ).format(min_year, max_year)
    
    # Rename various column headers
    headers_def_tackles = [
            'Player', 'Date', 'G', 'School', 'Loc', 'Opponent', 'WL', 
            'Tackle Solo', 'Tackle Assist', 'Tackle Tot', 'Tackle for Loss', 
            'Sack', 'unique_id']

    # set the filename
    filename_def_tackles = ''
    if min_year == max_year:
        filename_def_tackles = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_def_tackles.csv' % 
                (min_year))
    else:
        filename_def_tackles = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_to_%i_def_tackles.csv' % 
                    (min_year, max_year))

    # scrape data and write to disk for defensive stats
    print('###------- Starting Scraper for NCAA Defense (Tackles) -------###')
    run_scraper(filename_def_tackles, headers_def_tackles, url_def_tackles)

    #---------------------- INTERCEPTIONS--------------------------------------   
    # set the desired url for scraping interception statistics
    url_def_int = ('https://www.sports-reference.com/cfb/play-index/' + 
                   'pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&' + 
                   'c1stat=def_int&c1comp=gt&c1val=1&c2val=0&c3val=0&' + 
                   'c4val=0&order_by=date_game&order_by_asc=Y'
            ).format(min_year, max_year)
    
    # Rename various column headers
    headers_def_int = [
            'Player', 'Date', 'G', 'School', 'Loc', 'Opponent', 'WL', 
            'Int Ret', 'Int Ret Yds', 'Int TD', 'Pass Broken Up', 'unique_id']

    # set the filename
    filename_def_int = ''
    if min_year == max_year:
        filename_def_int = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_def_int.csv' % 
                (min_year))
    else:
        filename_def_int = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_to_%i_def_int.csv' % 
                    (min_year, max_year))

    # scrape data and write to disk for defensive stats
    print('###------- Starting Scraper for NCAA Defense (Interceptions) -------###')
    run_scraper(filename_def_int, headers_def_int, url_def_int)
    
    #---------------------- PASSES DEFEND -------------------------------------   
    # set the desired url for scraping passes broken up (PBU) statistics
    url_def_pbu = ('https://www.sports-reference.com/cfb/play-index/' + 
                   'pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&' + 
                   'c1stat=pass_defended&c1comp=gt&c1val=1&c2val=0&c3val=0&' + 
                   'c4val=0&order_by=date_game&order_by_asc=Y'
            ).format(min_year, max_year)
    
    # Rename various column headers
    headers_def_pbu = [
            'Player', 'Date', 'G', 'School', 'Loc', 'Opponent', 'WL', 
            'Int Ret', 'Int Ret Yds', 'Int TD', 'Pass Broken Up', 'unique_id']

    # set the filename
    filename_def_pbu = ''
    if min_year == max_year:
        filename_def_pbu = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_def_pbu.csv' % 
                (min_year))
    else:
        filename_def_pbu = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_to_%i_def_pbu.csv' % 
                    (min_year, max_year))

    # scrape data and write to disk for defensive stats
    print('###------- Starting Scraper for NCAA Defense (Passes Defended) -------###')
    run_scraper(filename_def_pbu, headers_def_pbu, url_def_pbu)
    
    # Merge Passes Defended and Interceptions into `def_pass`
    df_int = pd.read_csv(filename_def_int)
    df_pbu = pd.read_csv(filename_def_pbu)
    df_pass = pd.merge(df_int,
                       df_pbu,
                       how = 'outer',
                       on = ['Player', 'Date', 'G', 'School', 'Loc', 
                             'Opponent', 'WL', 'unique_id'])
        
    # merge duplicate variables
    for variable in ['Int Ret', 'Int Ret Yds', 'Int TD', 'Pass Broken Up']:
        # keep the _x version unless it is missing, then go with _y
        df_pass[variable] = df_pass.apply(lambda row:
            row['%s_x' % variable] if (pd.isna(row['%s_y' % variable]) 
            or row['%s_y' % variable] == '') else 
            row['%s_y' % variable], axis = 1)
    # remove old columns
    df_pass = df_pass[['Player', 'Date', 'G', 'School', 'Loc', 'Opponent', 
                       'WL', 'unique_id', 'Int Ret', 'Int Ret Yds', 'Int TD', 
                       'Pass Broken Up']]
    # write to disk
    filename_def_pass = ''
    if min_year == max_year:
        filename_def_pass = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_def_pass.csv' % 
                (min_year))
    else:
        filename_def_pass = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_to_%i_def_pass.csv' % 
                    (min_year, max_year))
    df_pass.to_csv(filename_def_pass, index = False)
        
    #---------------------- FORCED FUMBLES ------------------------------------   
    # set the desired url for scraping forced fumble statistics
    url_def_fumb = ('https://www.sports-reference.com/cfb/play-index/' + 
                   'pgl_finder.cgi?request=1&match=game&year_min={}&year_max={}&' + 
                   'c1stat=fumbles_forced&c1comp=gt&c1val=1&c2val=0&c3val=0&' + 
                   'c4val=0&order_by=date_game&order_by_asc=Y'
            ).format(min_year, max_year)
    
    # Rename various column headers
    headers_def_fumb = [
            'Player', 'Date', 'G', 'School', 'Loc', 'Opponent', 'WL', 
            'FR', 'FR Yds', 'FR TD', 'Fumble Forced', 'unique_id']

    # set the filename
    filename_def_fumb = ''
    if min_year == max_year:
        filename_def_fumb = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_def_fumb.csv' % 
                (min_year))
    else:
        filename_def_fumb = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_to_%i_def_fumb.csv' % 
                    (min_year, max_year))

    # scrape data and write to disk for defensive stats
    print('###------- Starting Scraper for NCAA Defense (Fumbles) -------###')
    run_scraper(filename_def_fumb, headers_def_fumb, url_def_fumb)
    
    return

def scrape_player_statistics_kicks(min_year, max_year):
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
        ncaa_player_2xxx_kick_xp.csv : csv file
            Contains statistics for every player who returned kicked extra 
            points across all games played for the specified time window.
        ncaa_player_2xxx_kick_fg.csv : csv file
            Contains statistics for every player who returned kicked field 
            goals across all games played for the specified time window.
    '''            
    #---------------------- EXTRA POINTS --------------------------------------
    # URL for scraping extra point data (kickers part 1)
    url_xp = (
            'https://www.sports-reference.com/cfb/play-index/pgl_finder.cgi?' + 
            'request=1&match=game&year_min={}&year_max={}&c1stat=xpa' + 
            '&c1comp=gt&c1val=1&c2val=0&c3val=0&order_by=date_game&order_by_asc=Y'
            ).format(min_year, max_year)

    # Rename various column headers
    headers_xp = [
            'Player', 'Date', 'G', 'School', 'Loc', 'Opponent', 'WL', 
            'XP Made', 'XP Att', 'XP %', 'FG Made', 'FG Att', 'FG %', 'Pts', 
            'unique_id']
    
    # set the filename
    filename_xp = ''
    if min_year == max_year:
        filename_xp = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_kick_xp.csv' 
                % (min_year))
    else:
        filename_xp = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_to_%i_kick_xp.csv' % 
                (min_year, max_year))

    # scrape data and write to disk for field goal stats
    print('###------- Starting Scraper for NCAA Kicking (Extra Points) -------###')
    run_scraper(filename_xp, headers_xp, url_xp)

    #---------------------- FIELD GOALS ---------------------------------------    
    # URL for scraping extra point data (kickers part 2)
    url_fg = ('https://www.sports-reference.com/cfb/play-index/pgl_finder.cgi?' + 
              'request=1&match=game&year_min={}&year_max={}&c1stat=fga' + 
              '&c1comp=gt&c1val=1&c2val=0&c3val=0&c4val=0&order_by=date_game' + 
              '&order_by_asc=Y').format(min_year, max_year)

    # Rename various column headers
    headers_fg = [
            'Player', 'Date', 'G', 'School', 'Loc', 'Opponent', 'WL', 
            'XP Made', 'XP Att', 'XP %', 'FG Made', 'FG Att', 'FG %', 'Pts', 
            'unique_id']
    
    # set the filename
    filename_fg = ''
    if min_year == max_year:
        filename_fg = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_kick_fg.csv' 
                % (min_year))
    else:
        filename_fg = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_to_%i_kick_fg.csv' % 
                (min_year, max_year))

    # scrape data and write to disk for extra point stats
    print('###------- Starting Scraper for NCAA Kicking (Field Goals) -------###')
    run_scraper(filename_fg, headers_fg, url_fg)
    
    #----------------------- MERGE KICKING DATA -------------------------------
    df_fg = pd.read_csv(filename_fg)
    df_xp = pd.read_csv(filename_xp)
    
    # Isolate players who only have field goal data (i.e. XP Attempts == 0)
    df_fg = df_fg[df_fg['XP Att'] == 0]      
    
    # merge the field-goal only data with extra point data for all kicking stats
    df_kicking = df_xp.append(df_fg)
 
    # set the filename
    filename_kicking = ''
    if min_year == max_year:
        filename_kicking = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_kicking.csv' % 
                (min_year))
    else:
        filename_kicking = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_to_%i_kicking.csv' % 
                (min_year, max_year))
        
    # sort by date
    df_kicking = df_kicking.sort_values(by = ['Date', 'Pts'])
        
    # output to disk        
    df_kicking.to_csv(filename_kicking, index = False)

    
    return

def scrape_player_statistics_returns(min_year, max_year):
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
        ncaa_player_2xxx_ret_punt.csv : csv file
            Contains statistics for every player who returned punts across all 
            games played for the specified time window.
        ncaa_player_2xxx_ret_kickoff.csv : csv file
            Contains statistics for every player who returned kickoffs across 
            all games played for the specified time window.
    '''         
    #---------------------- KICK RETURNS---------------------------------------
    # URL for scraping kick return data (special teams part 1)
    url_kick_ret = ('https://www.sports-reference.com/cfb/play-index/' + 
                    'pgl_finder.cgi?request=1&match=game&year_min={}' + 
                    '&year_max={}&c1stat=kick_ret&c1comp=gt&c1val=1&c2val=0' + 
                    '&c3val=0&order_by=date_game&order_by_asc=Y'
                    ).format(min_year, max_year, max_year)

    # Rename various column headers
    headers_kick_ret = [
            'Player', 'Date', 'G', 'School', 'Loc', 'Opponent', 'WL', 'Kick Ret', 
            'Kick Ret Yds', 'Kick Ret Avg', 'Kick Ret TD', 'unique_id']
    
    # set the filename
    filename = ''
    if min_year == max_year:
        filename = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_ret_kick.csv' 
                % (min_year))
    else:
        filename = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_to_%i_ret_kick.csv' % 
                (min_year, max_year))

    # scrape data and write to disk for kick return stats
    print('###------- Starting Scraper for NCAA Returns (Kicks) -------###')
    run_scraper(filename, headers_kick_ret, url_kick_ret)    

    #---------------------- PUNT RETURNS---------------------------------------    
    # URL for scraping punt return data (special teams part 1)
    url_punt_ret = ('https://www.sports-reference.com/cfb/play-index/' + 
                    'pgl_finder.cgi?request=1&match=game&year_min={}' + 
                    '&year_max={}&c1stat=punt_ret&c1comp=gt&c1val=1&c2val=0' + 
                    '&c3val=0&order_by=date_game&order_by_asc=Y'
                    ).format(min_year, max_year, max_year)
    
    # Rename various column headers
    headers_punt_ret = [
            'Player', 'Date', 'G', 'School', 'Loc', 'Opponent', 'WL', 'Punt Ret', 
            'Punt Ret Yds', 'Punt Ret Avg', 'Punt Ret TD', 'unique_id']
    
    # set the filename
    filename = ''
    if min_year == max_year:
        filename = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_ret_punt.csv' 
                % (min_year))
    else:
        filename = (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%i_to_%i_ret_punt.csv' % 
                (min_year, max_year))

    # scrape data and write to disk for punt return stats
    print('###------- Starting Scraper for NCAA Returns (Punts) -------###')
    run_scraper(filename, headers_punt_ret, url_punt_ret)
    
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
        ncaa_team_data_2xxx.csv : csv file
            Contains team-based statistics for every team across all games 
            played (specifically passing and rushing statistics). For every
            game, there will be 2 rows (1 for each team) unless one of the
            teams is not FBS (in which case their will be no row for that team)
    '''    
    # set the desired url for scraping purposes using the input date range   
    url = ('https://www.sports-reference.com/cfb/play-index/sgl_finder.cgi?' +
            'request=1&match=game&year_min={}&year_max={}&c1stat=pass_att&' +
            'c1comp=gt&c1val=0&c2stat=rush_att&c2comp=gt&c2val=0&' + 
            'c3stat=points&c3comp=gt&c3val=0&c4stat=tot_plays&c4comp=gt&' + 
            'c4val=0&order_by=date_game&order_by_asc=Y').format(min_year, max_year)
    
    # Rename various column headers
    headers = ['School', 'Date', 'G', 'Loc', 'Opponent', 'WL', 'Pass Comp', 
               'Pass Att', 'Pass Pct', 'Pass Yard', 'Pass TD', 'Rush Att', 
               'Rush Yard', 'Rush Avg', 'Rush TD', 'Total Plays', 'Total Yards',
               'Total Yards Avg', 'Total TD', 'Total Points', 'Point Diff']
    
    # set the filename
    filename = ''
    if min_year == max_year:
        filename = 'Data/sports_ref/ncaa_team/ncaa_team_data_%i.csv' % (min_year)
    else:
        filename = ('Data/sports_ref/ncaa_team/ncaa_team_data_%i_to_%i.csv' % 
                    (min_year, max_year))
    
    # scrape data and write to disk
    print('###------- Starting Scraper for NCAA Team Game Statistics -------###')
    run_scraper(filename, headers, url, team = True)
    
    # format the scraped team stats
    format_team_statistics(min_year)
    
    return

def set_team_code(list_teams, year):
    '''
    Purpose: Determine the NCAA team code for each player's respective
        school and the code of their opponent

    Inputs
    ------
        list_teams : list of strings
            list of teams that require team codes
        year : int
            year for which data is being altered
            
    Outputs
    -------
        list_codes : list of ints
            list of codes for each team
    '''
    # read in team dataframe
    df_teams = pd.read_csv('Data/teams_ncaa.csv')

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

def merge_files_for_year(year):
    '''
    Purpose: Merge datasets together based on player ID to create a single file

    Inputs
    ------
        year : int
            Year for which data is to be combined
    
    Outputs
    -------
        ncaa_player_2xxx.csv : csv file
            Contains all player-based statistics for all scraped categories, 
            written to disk in a .csv file format
    ''' 
    print('###------- Merging files for %i -------###' % year)
    
    # create the master dataframe
    df_master = pd.DataFrame()
    
    # iterate through all files and merge data together    
    for file in sorted(glob.glob(
            'Data/sports_ref/ncaa_player/sub_stats/*%s*.csv' % str(year))):
        # ignore any existing master file
        if file == (
                'Data/sports_ref/ncaa_player/sub_stats/ncaa_player_%s.csv' % 
                str(year)):
            continue
        # ignore the kick_fg and kick_xp files --> data contained in kicking
        elif ('kick_fg' in file) or ('kick_xp' in file):
            continue
        # ignore the `int` and `pbu` files as they have been merged into `def_pass`
        elif ('int' in file) or ('pbu' in file):
            continue
        # otherwise read in the file
        else:
            df_file = pd.read_csv(file)
        
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
                    left_on = ['Player', 'Date', 'G', 'School', 'Loc', 
                               'Opponent', 'WL', 'unique_id'],
                    right_on = ['Player', 'Date', 'G', 'School', 'Loc', 
                               'Opponent', 'WL', 'unique_id'])          
            
            # check to make sure all player data is transferred over
            list_ids = df_master['unique_id'].tolist()
            for new_id in df_file['unique_id']:
                if new_id not in list_ids:
                    print('ID not found in master: %s' % new_id)

    df_master = df_master.rename(columns = {'Tackle For Loss':'Tackle for Loss'})

    # insert team codes for each team and their respective opponent
    df_master['Team Code'] = set_team_code(df_master['School'].tolist(), year)
    df_master['Team Code opp'] = set_team_code(
            df_master['Opponent'].tolist(), year)    
    
    # standardize team names for the player's team
    df_master = rename_school(df_master, 'School')
    # standardize team names for the player's opponent
    df_master = rename_school(df_master, 'Opponent')

    #------------ Add player position data to dataframe ----------------------#
    # ingest the player metadata
    df_meta = pd.read_csv(
            'Data/sports_ref/ncaa_player_meta/player_lookup_ncaa.csv')[
            ['id_ncaa', 'pos_ncaa_std']]
    df_meta = df_meta.rename(columns = {'id_ncaa':'unique_id', 
                                        'pos_ncaa_std' : 'position'})

    # add player position data to the player stats
    df_master = pd.merge(df_master,
                         df_meta,
                         how = 'left',
                         left_on = 'unique_id',
                         right_on = 'unique_id')
    
    # fill in missing position information with an empty array
    df_master['position'] = df_master['position'].apply(
            lambda x: '[]' if pd.isna(x) else x)
    
    # remove any extra spaces from the player_id variable to prevent hanging spaces
    df_master['unique_id'] = df_master['unique_id'].apply(
            lambda x: x.strip() if type(x) == str else x)
    
    # reorder columns
    if  'Tackle Solo' not in df_master.columns:
        df_master = df_master[[
                'Player', 'unique_id', 'position' ,'Date', 'G', 'School', 
                'Team Code', 'Loc', 'Opponent', 'Team Code opp',
                'WL', 'Pass Comp', 'Pass Att', 'Pass Pct', 'Pass Yard', 'Pass TD', 
                'Pass Int', 'QBR', 'Rush Att', 'Rush Yard', 'Rush Avg', 'Rush TD', 
                'Rec', 'Rec Yards', 'Rec Avg', 'Rec TD',  
                'Int Ret', 'Int Ret Yds', 'Int TD', 'Pass Broken Up', 
                'XP Made', 'XP Att', 'XP %', 'FG Made', 'FG Att', 
                'FG %', 'Pts', 'Kick Ret', 'Kick Ret Yds', 'Kick Ret Avg', 
                'Kick Ret TD', 'Punt Ret', 'Punt Ret Yds', 'Punt Ret Avg', 
                'Punt Ret TD']]
    else:
        df_master = df_master[[
                'Player', 'unique_id', 'position' ,'Date', 'G', 'School', 
                'Team Code', 'Loc', 'Opponent', 'Team Code opp',
                'WL', 'Pass Comp', 'Pass Att', 'Pass Pct', 'Pass Yard', 'Pass TD', 
                'Pass Int', 'QBR', 'Rush Att', 'Rush Yard', 'Rush Avg', 'Rush TD', 
                'Rec', 'Rec Yards', 'Rec Avg', 'Rec TD', 'Tackle Solo', 
                'Tackle Assist', 'Tackle Tot', 'Tackle for Loss', 'Sack', 'Int Ret', 
                'Int Ret Yds', 'Int TD', 'Pass Broken Up', 'FR', 'FR Yds', 'FR TD', 
                'Fumble Forced', 'XP Made', 'XP Att', 'XP %', 'FG Made', 'FG Att', 
                'FG %', 'Pts', 'Kick Ret', 'Kick Ret Yds', 'Kick Ret Avg', 
                'Kick Ret TD', 'Punt Ret', 'Punt Ret Yds', 'Punt Ret Avg', 
                'Punt Ret TD']]
    
    # output to disk        
    df_master.to_csv('Data/sports_ref/ncaa_player/ncaa_player_%i.csv' % (year), 
                     index = False)

    print('###------- Completed merge for %i -------###' % year)
        
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
        ncaa_team_data_2xxx.csv : csv file
            Contains all player-based statistics for all scraped categories, 
            written to disk in a .csv file format
    ''' 
    filename = 'Data/sports_ref/ncaa_team/ncaa_team_data_%s.csv' % str(year)
    df_year = pd.read_csv(filename)

    # insert team codes for each team and their respective opponent
    df_year['Team Code'] = set_team_code(df_year['School'].tolist(), year)
    df_year['Team Code opp'] = set_team_code(df_year['Opponent'].tolist(), year)    
    
    # standardize team names for the player's team
    df_year = rename_school(df_year, 'School')
    # standardize team names for the player's opponent
    df_year = rename_school(df_year, 'Opponent')
    
    # set desired order of columns
    df_year = df_year[[
            'School', 'Team Code', 'Date', 'G', 'Loc', 'Opponent', 
            'Team Code opp', 'WL', 'Pass Comp', 'Pass Att', 'Pass Pct', 
            'Pass Yard', 'Pass TD', 'Rush Att', 'Rush Yard', 'Rush Avg', 
            'Rush TD', 'Total Plays', 'Total Yards', 'Total Yards Avg', 
            'Total TD', 'Total Points', 'Point Diff']]
    
    # output file to disk
    df_year.to_csv('Data/sports_ref/ncaa_team/ncaa_team_data_%s.csv' % 
                   str(year), index = False)
    
    return

def main():
    pass

#=============================================================================
# Working Code
#==============================================================================
if __name__ == "__main__":
    main()

# Set the project working directory
#path_dir = pathlib.Path('/home/ejreidelbach/Projects/draft-analytics-api/src/data')
#os.chdir(path_dir)
#
## scrape data for the specified years
#year = 2019
#scrape_player_statistics_offense(year, year)
#scrape_player_statistics_defense(year, year)
#scrape_player_statistics_kicks(year, year)
#scrape_player_statistics_returns(year, year)
#merge_files_for_year(year)
#scrape_team_statistics(year, year)
#format_team_statistics(year)

# iterate over every year from 2005 to 2019 and scrape 
#for year in range(2003,2020):
#    scrape_player_statistics_offense(year, year)
#    scrape_player_statistics_defense(year, year)
#    scrape_player_statistics_kicks(year, year)
#    scrape_player_statistics_returns(year, year)
#    merge_files_for_year(year)
#
# iterate over every year from 2000 to 2019 and scrape date
#for year in range(2000,2020):
#    scrape_team_statistics(year, year)
#    format_team_statistics(year)