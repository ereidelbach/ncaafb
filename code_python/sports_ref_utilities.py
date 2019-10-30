#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 09:40:52 2019

@author: ejreidelbach

:DESCRIPTION:

:REQUIRES:
    Contains standardized functions utilized in the sports reference 
    
:TODO:
    TBD
"""
 
#==============================================================================
# Package Import
#==============================================================================
import csv
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

#==============================================================================
# Working Code
#==============================================================================

# Set the project working directory
path_dir = pathlib.Path('/home/ejreidelbach/Projects/')
os.chdir(path_dir)
