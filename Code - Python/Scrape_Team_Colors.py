#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 29 10:36:39 2017

@author: ejreidelbach
"""

#------------------------------------------------------------------------------
# Obtain Color Code Information for each team
# -----------------------------------------------------------------------------

# Import the Required Libraries
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import string

###############################################################################
# Function Definitions

def site_to_soup(url):
    headers = {"User-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.content,'html.parser')   
    return soup

###############################################################################
# Working Code

colors_url = 'http://www.codeofcolors.com/college-football-team-colors.html'

colorList = []

# Scrape the main page to identify all links for each team
soup = site_to_soup(colors_url)
teamSection = soup.findAll('tbody')
teamURLs = teamSection[0].findAll(href=True)
teamNames = []
teamLinks = []
for team in teamURLs:
    teamLinks.append('http://www.codeofcolors.com/' + str(team['href']))
    name = string.capwords(str(team['href']).split('-colors.html',1)[0].replace('-',' '))
    teamNames.append(name)

# Scrape each team's page to retrieve team-specific colors
for link, teamName in zip(teamLinks, teamNames):
    print('Extracting data for: ' + str(teamName))
    linkSoup = site_to_soup(link)
    teamInfo = []
    teamInfo.append(teamName)
    teamColor = linkSoup.findAll('div', {'class':'color-box'})
    for color in teamColor:
        # Extract Hex Color Combo
        hex = color.text.split(' - ')[0].strip()
        teamInfo.append(hex)
        # Extract RGB Color Combo
        rgb = color.text.split('rgb')[1].replace('(','').replace(')','').replace(' ','')
        teamInfo.append(rgb)
    
    # Add info to master list
    colorList.append(teamInfo)

# Insert missing D1 schools
colorList.append(['Charlotte','#00703C','0, 112, 60','#ffffff','255,255,255'])
colorList.append(['Coastal Carolina','#006f71','0,111,113','#a27752','162,119,82','#111111','17,17,17'])
colorList.append(['Old Dominion','#003057','0,48,87','#7c878e','124,135,142','#92c1e9','146,193,233'])
colorList.append(['UAB','#1E6B52','30,107,82','#F4C300','244,195,0'])

# Convert master list to a master dataframe
colorDF = pd.DataFrame(colorList)

# Rename columns of the datarame
column_names = ['School','Hex1','RGB1','Hex2','RGB2','Hex3','RGB3','Hex4','RGB4']
colorDF.columns = column_names
colorDF.sort_values(['School'], inplace=True)

# Rename schools to align with other sites naming schemes
# 22 teams don't have team names (`list_orig`) that match the official NCAA stats page (`list_new`)
list_orig = ['Byu','Fau','Fiu','Louisiana Lafayette','Louisiana Monroe',
             'Lsu','Umass','Miami','Miami of Ohio','Nc State','Mississippi',
             'San Diego','San Jose','Smu','Tcu','Texas Am','Ucf','Ucla','Unlv',
             'Usc','Utep','Utsa']
list_new  = ['Brigham Young','Florida Atlantic','Florida International',
             'Louisiana-Lafayette','Louisiana-Monroe','LSU','Massachusetts',
             'Miami (FL)','Miami (OH)','North Carolina State','Ole Miss',
             'San Diego State','San Jose State','Southern Methodist','TCU',
             'Texas A&M','UCF','UCLA','UNLV','USC','UTEP','UTSA']

# Output dataframe to a CSV
os.chdir('/home/ejreidelbach/projects/CollegeFootball/CSVs')
colorDF.to_csv('Info/team_colors.csv', index=False)