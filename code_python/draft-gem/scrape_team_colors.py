#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 14:40:07 2019

@author: ejreidelbach

:DESCRIPTION: Scrapes team colors and adds data to 'teams_ncaa.csv' and
    'teams_nfl.csv'.  Colors are sourced from:
    https://usteamcolors.com/

:REQUIRES:
   
:TODO:
"""
 
#==============================================================================
# Package Import
#==============================================================================
import ast
import colorsys
import html
import os  
import pandas as pd
import pathlib
import requests
import time
import tqdm

from bs4 import BeautifulSoup
from requests.packages.urllib3.util.retry import Retry
from code_python.standardize_names_and_logos import rename_teams

#==============================================================================
# Reference Variable Declaration
#==============================================================================

#==============================================================================
# Function Definitions
#==============================================================================
def soupify_url(url):
    '''
    Purpose: Turns a specified URL into BeautifulSoup formatted HTML 

    Inputs
    ------
        url : string
            Link to the designated website to be scraped
    
    Outputs
    -------
        soup : html
            BeautifulSoup formatted HTML data stored as a complex tree of 
            Python objects
    '''
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    r = session.get(url)
    #r = requests.get(url)
    soup = BeautifulSoup(r.content,'html.parser')   
    return soup

def scrape_team(url_team, league):
    '''
    Purpose: Scrapes the colors for an individual team.

    Inputs
    ------
        url_team : string
            hyperlink to an individual team's color page
        league : string
            Specifies the type of teams being scraped (i.e. 'nfl' or 'ncaa')      

    Outputs
    -------
        dict_team : dictionary
             Contains the name of the team, the name of their colors (as well
             as the RGB and Hex value of each color), and the url of the 
             team's logo
    '''
    # scrape the team's page
    soup = soupify_url(url_team) 
    while '503 Service' in str(soup):
        print('Connection error: Sleeping 10 seconds')
        time.sleep(10)
        soup = soupify_url(url_team) 
    
    dict_team = {}

    # extract the full team name and standardize it
    team_name = soup.find('div',{'class':'description'}).find('h2')
    team_name = html.unescape(str(team_name)).split(' color codes')[0].replace('<h2>','')
    dict_team['Team'] = rename_teams([team_name], league)[0]

    # isolate the colors and logo information
    team_info = soup.find('div', {'class':'colors'})
    
    list_colors = []
    colors = team_info.find_all('tbody')
    for color in colors:
        dict_color = {}
        dict_color['color'] = color.find('th').text
        dict_color['hex'] = color.find_all('tr')[0].find_all('td')[1].text.strip()
        dict_color['rgb'] = color.find_all('tr')[1].find_all('td')[1].text.strip()
        dict_color['rgb'] = dict_color['rgb'].replace(' ',', ')
        list_colors.append(dict_color)
    dict_team['colors'] = list_colors

    dict_team['logo'] = team_info.find('img')['src']

    return dict_team

def scrape_team_links(league):
    '''
    Purpose: Obtain the links to each team's profile page

    Inputs
    ------
        league : string
            Specifies the type of teams being scraped (i.e. 'nfl' or 'ncaa')        
        
    Outputs
    -------
        list_links : list of strings
            List of links to individual team pages
    '''   
    list_links = []
    
    if league == 'nfl':
        url = f'https://usteamcolors.com/nfl-colors/'
        soup = soupify_url(url)
        
        # isolate the team cards and extract their info
        main_page = soup.find('div', {'class':'us-teams'})
        teams = main_page.find_all('li', {'class':'card'})
        
        # extract the team name and url for each team's page
        for team in teams:
            list_links.append(team.find('a')['href'])
            
    elif league == 'ncaa':
        for url in ['https://usteamcolors.com/ncaa-division-1/',
                    'https://usteamcolors.com/ncaa-division-2/']:
            soup = soupify_url(url)
        
            # isolate the links for each conference
            main_page = soup.find('div', {'class':'us-teams'})
            conferences = main_page.find_all('li', {'class':'card'})
            list_conf_urls = [x.find('a')['href'] for x in conferences]
            
            for conf_url in tqdm.tqdm(list_conf_urls):     
                soup_conf = soupify_url(conf_url)
                while '503 Service' in str(soup_conf):
                    print('Connection error: Sleeping 10 seconds')
                    time.sleep(10)
                    soup_conf = soupify_url(conf_url)
                
                # isolate the links for each conference
                main_page_conf = soup_conf.find('div', {'class':'us-teams'})
                teams = main_page_conf.find_all('li', {'class':'card'})
                
                # extract the team name and url for each team's page
                for team in teams:
                    list_links.append(team.find('a')['href'])  
                    
                time.sleep(3)
    
    return list_links

def scrape_colors(league):
    '''
    Purpose: Scrapes the colors for an individual team.

    Inputs
    ------
        league : string
            Specifies the type of teams being scraped (i.e. 'nfl' or 'ncaa')
    
    Outputs
    -------
        NONE 
            No data is output -- it is simply merged with the respective
            league file already existing on the disk (i.e. 'teams_ncaa.csv' or
            'teams_nfl.csv')
    '''
    #-------------- NCAA Colors ----------------------------------------------#
    # set the URL of the base website

    # obtain the links for each team page
    list_links = set(scrape_team_links(league))
    
    # scrape the colors for each team
    list_teams = []
    for link in tqdm.tqdm(list_links):
        list_teams.append(scrape_team(link, league))
        time.sleep(3)

    # create an hls (hue, lightness, saturation) value for each color
    def rgb_to_hls(rgb):
        '''
            Given a RGB value for a color, return the HLS value of that color
        '''
        try:
            rgb = ast.literal_eval(rgb)
            return str(colorsys.rgb_to_hls(rgb[0], rgb[1], rgb[2]))[1:-1]
        except:
            print(rgb)

    # convert list to dataframe
    list_teams_unpacked = []
    for team in list_teams:
        dict_team = {}
        dict_team['Team'] = team['Team']
        dict_team['logo'] = team['logo']
        count = 0
        for color in team['colors']:
            dict_team[f'color_{count}'] = color['color']
            dict_team[f'color_{count}_rgb'] = color['rgb']
            dict_team[f'color_{count}_hex'] = color['hex']
            dict_team[f'color_{count}_hls'] = rgb_to_hls(color['rgb'])
            count = count + 1
        list_teams_unpacked.append(dict_team)
    df_colors = pd.DataFrame(list_teams_unpacked)

    # fill in missing values
    df_colors = df_colors.fillna('')
    
#    # standardize team names
#    df_colors['Team'] = df_colors['Team'].apply(lambda x: html.unescape(x))
#    df_colors['Team'] = rename_teams(list(df_colors['Team']), league)
#    df_colors['Team'] = df_colors['Team'].apply(lambda x: str(x).strip())

    # import existing team data
    df_lookup = pd.read_csv(f'data_team/teams_{league}.csv')
    
    # add color data to team data
    df_merged = pd.merge(df_lookup, df_colors, how = 'left', on = 'Team')
       
    # fill in missing values
    df_merged = df_merged.fillna('')
            
#    df_merged['color_0_hls'] = df_merged['color_0_hex'].apply(lambda x:
#        rgb_to_hsl(x) if x != '' else x)
#    df_merged['color_1_hls'] = df_merged['color_1_hex'].apply(lambda x:
#        rgb_to_hsl(x) if x != '' else x)
#    df_merged['color_2_hls'] = df_merged['color_2_hex'].apply(lambda x:
#        rgb_to_hsl(x) if x != '' else x)
        
    # write to disk    
    df_merged.to_csv(f'data_team/teams_{league}_new.csv', index = False)        
        
    return
            
    
#==============================================================================
# Working Code
#==============================================================================

# Set the project working directory
path_dir = pathlib.Path('/home/ejreidelbach/Projects/draft-analytics-api/src/data')
os.chdir(path_dir)

# scrape NCAA colors
scrape_colors('ncaa')

# scrape NFL colors
scrape_colors('nfl')

#    # scrape the links to each conference
#    soup = soupify_url(url)
#    
#    # isolate section of page containing team links
#    div = soup.find('div', {'class':'entry-content'})
#    teams = BeautifulSoup(str(div).split('Browse By Team')[1], 'html.parser')
#    
#    # scrape the links for each team
#    list_teams = []
#    for team in teams.find_all('a'):
#        dict_team = {}
#        dict_team['team'] = team.text
#        dict_team['url'] = team['href']
#        list_teams.append(dict_team)
#    
#    # scrape the colors for each team
#    for team in tqdm.tqdm(list_teams):
#        team['colors'] = scrape_team(team)
#        
#    # convert list to dataframe
#    list_rows = []
#    for team in list_teams:
#        for color in team['colors']:
#            dict_color = color
#            dict_color['team'] = team['team']
#            list_rows.append(dict_color)
#    df_colors = pd.DataFrame(list_rows)
#    
#    # fill in missing values
#    df_colors = df_colors.fillna('')
#    
#    # standardize team names
#    df_colors['team'] = rename_teams(list(df_colors['team']), league)
#    df_colors['team'] = df_colors['team'].apply(lambda x: str(x).strip())
#    df_colors = df_colors.rename(columns = {'team':'Team'})
#    
#    # collapse dataframe rows and transform into columns
#    list_rows = []
#    for team in df_colors['Team'].drop_duplicates():
#        team_series = pd.Series(team, index = ['Team'])
#        count = str(0)
#        for idx, row in df_colors[df_colors['Team'] == team].iterrows():
#            row_color = row[0:4]
#            row_color.index = [f'color{count}_name', f'color{count}_hex',
#                               f'color{count}_rgb', f'color{count}_cmyk']
#            team_series = team_series.append(row_color)
#            count = str(int(count) + 1)
#        list_rows.append(team_series)
#    df_colorsV2 = pd.DataFrame(list_rows)

#def scrape_team(dict_team):
#    '''
#    Purpose: Scrapes the colors for an individual team.
#
#    Inputs
#    ------
#        team_dict : dictionary
#            Contains two keys:
#                team - the name of the team being scraped
#                url - the address of the team's color page
#    
#    Outputs
#    -------
#        list_colors : list of dictionaries
#            Contains a complete listing of all team-specific color values
#    '''
#    # scrape the team's page
#    soup = soupify_url(dict_team['url'])
#    colors = soup.find_all('div', {'class':'colorblock'})
#    
#    # set characters to be removed
#    remove_table = str.maketrans("","",';()')
#    
#    # extract all the colors for a given team
#    list_colors = []
#    # iterate over every color a team has
#    for color in colors:
#        dict_color = {}
#        dict_color['color'] = str(color).split('<br/>')[0].split('>')[1].strip().title()
#        if ':' in dict_color['color']:
#            dict_color['color'] = dict_color['color'].split(':')[0].strip()
#        if '<' in dict_color['color']:
#            dict_color['color'] = dict_color['color'].split('<')[0].strip()
#        if 'hex color' in dict_color['color'].lower():
#            dict_color['color'] = dict_color['color'].lower().replace('hex color','').strip().title()
#        
#        # iterate over every available code option for the team
#        if '<br/>' in str(color):
#            color_codes = str(color).split('<br/>')
#                       
#            for code in color_codes:
##                # handle pantone
##                if ('pantone' in code.lower()) and ('a href' not in code.lower()):
##                    try:
##                        dict_color['pantone'] = code.split(':')[1].strip()
##                    except:
##                        dict_color['pantone'] = code.split('PANTONE ')[1].strip()
#                # handle hex
#                if 'hex' in code.lower():
#                    try:
#                        dict_color['hex'] = code.split(':')[1].replace(
#                                ';','').strip().replace('</div>','')
#                    except:
#                        dict_color['hex'] = code.split('Color')[1].replace(
#                                ';','').strip().replace('</div>','')
#                # handle rgb
#                elif 'rgb' in code.lower():
#                    if ':' not in code:
#                        dict_color['rgb'] = list(ast.literal_eval(code.split(
#                                'rgb')[1].replace('</div>','')))
#                    else:
#                        try:
#                            dict_color['rgb'] = list(ast.literal_eval(
#                                    code.split(':')[1].strip().translate(remove_table).strip()))
#                        except:
#                            if ':' in code:
#                                value = code.split(':')[1].strip().replace(';','')
#                                value = value.replace('</div>','')
#                            
#                                try:
#                                    dict_color['rgb'] = list(ast.literal_eval(value))
#                                except:
#                                    dict_color['rgb'] = value.translate(remove_table).split(' ')
#                            else:
#                                value = list(ast.literal_eval(code.split('rgb')[
#                                        1].replace(';','').replace('</div>','')))
#                # handle cmyk
#                elif 'cmyk' in code.lower():
#                    if ':' not in code:
#                        dict_color['cmyk'] = list(ast.literal_eval(code.split(
#                                'CMYK ')[1].split('<')[0]))
#                    else:
#                        try:
#                            dict_color['cmyk'] = list(ast.literal_eval(code.split(
#                                    ':')[1].split('<')[0].translate(remove_table).strip()))
#                        except:
#                            value = code.split(':')[1].translate(
#                                    remove_table).strip().replace('</div>','')
#                            if ' ' in value:
#                                dict_color['cmyk'] = value.replace(',','').split(' ')
#                            else:
#                                dict_color['cmyk'] = value.split(',')
#                # handle other formatting
#                else:
#                    if 'Buy Matching Paint' in code:
#                        pass
#                    elif ('#' in code) and ('hex' not in dict_color.keys()):
#                        dict_color['hex'] = '#' + (code.split('#')[1].translate(
#                        remove_table).strip().split(' ')[0])
#                    else:
#                        try:
#                            dict_color['hex'] = color['style'].split(
#                                'background-color:')[1].split(';')[0].strip()
#                        except:
#                            print(code)
#        else:
#            # handle HEX
#            if '#' in str(color):
#                dict_color['hex'] = '#' + str(color).split('#')[1].strip().split(';')[0]
#            # handle RGB
#                if 'rgb' in str(color).lower():
#                    value = str(color).lower().split('rgb')[1].replace(':','').strip()
#                    value = value.replace('</div>','')
#                    try:
#                        dict_color['rgb'] = list(ast.literal_eval(value))
#                    except:
#                        print('Error: ' + value)
#                    
#        # ensure values in `rgb` and `cmyk` are ints
#        if 'rgb' in dict_color.keys():
#            dict_color['rgb'] = [int(x) for x in dict_color['rgb']]
#        if 'cmyk' in dict_color.keys():
#            dict_color['cmyk'] = [int(x) for x in dict_color['cmyk']]
#    
#        list_colors.append(dict_color)
#
#    return list_colors