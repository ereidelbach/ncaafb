#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 10:16:25 2019

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
import os  
import matplotlib.pyplot as plt
import pandas as pd
import pathlib
import re

from code_python.standardize_names_and_logos import rename_teams
from matplotlib.offsetbox import OffsetImage, AnnotationBbox

#==============================================================================
# Reference Variable Declaration
#==============================================================================

#==============================================================================
# Function Definitions
#==============================================================================  
def clean_sp_rankings(df_sp):
    '''
    Purpose: Read in raw S&P rankings and do some minor data wrangling to 
        extract ranks for each stastitical category

    Inputs   
    ------
        df_sp : Pandas DataFrame
            Contains raw S&P rankings from ESPN's weekly rankings
            - Example: https://www.espn.com/college-football/story/_/id/27940007/
                        sp+-rankings-week-9-lsu-catch-ohio-state-alabama
            
    Outputs
    -------
        --- : Pandas DataFrame
            Contains updated S&P information with rankings for each category
    '''
    list_rows = []
    for index, row in df_sp.iterrows():
        dict_team = {}
        # split team and record from team (record)
        # -- isolate team
        dict_team['team'] = re.split('^\d+\.?\s', row['Team (Record)'])[1].split(' (')[0]
        dict_team['record'] = row['Team (Record)'].split(' (')[1].split(')')[0]
        dict_team['wins'] = int(dict_team['record'].split('-')[0])
        dict_team['losses'] = int(dict_team['record'].split('-')[1])
        dict_team['overall'] = row['Rating']
        dict_team['overall_rank'] = index + 1
        dict_team['offense'] = float(row['Offense'].split(' ')[0])
        dict_team['offense_rank'] = int(row['Offense'].split('(')[1].split(')')[0])
        dict_team['defense'] = float(row['Defense'].split(' ')[0])
        dict_team['defense_rank'] = int(row['Defense'].split('(')[1].split(')')[0])
        dict_team['special_teams'] = float(row['Special Teams'].split(' ')[0])
        dict_team['special_teams_rank'] = int(row['Special Teams'].split('(')[1].split(')')[0])
        list_rows.append(dict_team)
    
    return pd.DataFrame(list_rows)

def ingest_sp_data():
    '''
    Purpose: Read in raw S&P data, clean it, standardize team names & merge
        with logo and conference information

    Inputs   
    ------
        NONE
            
    Outputs
    -------
        df_sp : Pandas DataFrame
            Contains finalized s&p rankings for all 130 FBS teams
    '''
    # ingest s&p rankings
    df_sp = pd.read_csv('/home/ejreidelbach/Projects/CollegeFootball/' + 
                        'Data/S&P_2019_week9.csv')
    # clean s&p rankings
    df_sp = clean_sp_rankings(df_sp)
    # standardize team names
    df_sp['team'] = rename_teams(df_sp['team'], 'ncaa')
    # import team metadata
    df_team = pd.read_csv('/home/ejreidelbach/Projects/CollegeFootball/' + 
                          'Data/teams_ncaa.csv',
                          usecols = ['Team', 'logo', 'Conference'])
    df_team = df_team.rename(columns = {'Team':'team', 
                                        'Conference':'conference'})
    # merge team logos into s&p data
    df_sp = pd.merge(df_sp, 
                     df_team,
                     how = 'left',
                     left_on = 'team',
                     right_on = 'team')

    # overwrite logo column
    df_sp['logo'] = df_sp.apply(
            lambda row: 'Images/logos_ncaa/' + row['team'] + '.png', axis = 1)
    
    return df_sp

def getImage(path):
    return OffsetImage(plt.imread(path))

#==============================================================================
# Working Code
#==============================================================================

# Set the project working directory
path_dir = pathlib.Path('/home/ejreidelbach/Projects/CollegeFootball')
os.chdir(path_dir)

# ingest s&p data
df_sp = ingest_sp_data()

# subset by conference
conference = 'Big Ten'
df_conf = df_sp[df_sp['conference'] == conference]

# set plot styles
#plt.rcParams['font.size'] = 10
#plt.rcParams['axes.labelsize'] = 10
#plt.rcParams['axes.labelweight'] = 'bold'
#plt.rcParams['axes.titlesize'] = 10
#plt.style.use('fivethirtyeight')

# extract data to be plotted
x = df_conf['offense']
y = df_conf['defense']
logos = df_conf['logo']

# create the plot
fig, ax = plt.subplots(figsize = (7, 7))
ax.scatter(x, y, alpha = 0.5, color = 'grey')

# turn on gridle lines
ax.grid(True, alpha = 0.2, color = 'black', linestyle = '-.')

# set plot styles
plt.rcParams['font.family'] = 'serif'
plt.rcParams['font.serif'] = 'Ubuntu'
plt.rcParams['font.monospace'] = 'Ubuntu Mono'
plt.rcParams['xtick.labelsize'] = 12
plt.rcParams['ytick.labelsize'] = 12
plt.rcParams['figure.titlesize'] = 12

# add title and subtitle
title_x = min(x) - 2
title_y = min(y) - 3
plt.text(x = title_x,
         y = title_y,
         s = f'{conference} S&P Rankings',
         fontsize = 26,
         weight = 'bold',
         alpha = 0.75)
# add subtitle
title_x = min(x) - 2
title_y = min(y) - 1.5 
plt.text(x = title_x,
         y = title_y,
         s = 'October 28, 2019 (Week 9)',
         fontsize = 19,
         alpha = 0.85)

# invert y axis
ax.set_ylim(ax.get_ylim()[::-1])
#ax.spines['right'].set_visibile(True)
#ax.spines['bottom'].set_visibile(True)

# set axis labels
ax.set_xlabel('Offensive S&P Rating', fontsize = 17, weight = 'bold', alpha = 0.85)
ax.set_ylabel('Defensive S&P Rating', fontsize = 17, weight = 'bold', alpha = 0.85)

# add signature bar
sig_x = min(x) - 5
sig_y = max(y) + 4.5
plt.text(x = sig_x,
         y = sig_y, 
         s = ('_______________________________________________________' 
              + '___________________________________'),
         color = 'grey', 
         alpha = 0.7)

# add signature text
plt.text(x = sig_x, 
         y = sig_y + 1,
         s = ('©Stewmanji                         ' + 
              'Source: Bill Connelly SP+ Week 9 Rankings (ESPN)'),
         fontsize = 13, 
         color = 'black', 
         alpha = 0.7)

# draw the images over the plots
for x0, y0, logo in zip(x, y, logos):
    ab = AnnotationBbox(getImage(logo), (x0, y0), frameon=False)
    ax.add_artist(ab)