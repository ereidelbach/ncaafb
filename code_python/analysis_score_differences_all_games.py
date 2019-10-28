#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 13 13:01:29 2018

@author: ejreidelbach

:DESCRIPTION:

:REQUIRES:
   
:TODO:
"""
 
#==============================================================================
# Package Import
#==============================================================================
import numpy as np
import os  
import pandas as pd
import pathlib

import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter

#==============================================================================
# Function Definitions / Reference Variable Declaration
#==============================================================================
def function_name(var1, var2, var3):
    '''
    Purpose: Stuff goes here

    Input:   
        (1) var1 (type): description
        (2) var2 (type): description
        (3) var3 (type): description
    
    Output: 
        (1) output1 (type): description
    '''
#==============================================================================
# Working Code
#==============================================================================

# Set the directory where the files are located
dir_path = pathlib.Path('/home/ejreidelbach/Projects/CollegeFootball/Data/CFBStats')

# Find every folder in the Data directory
#list_files_schedules = [file for file in dir_path.resolve().glob('**/*') if file.is_file()]
list_files_schedules = list(dir_path.rglob('schedules.csv'))

# Numpy style
array_score_differences = np.array([])
for file in list_files_schedules:
    df = pd.read_csv(file, index_col = 0)
    for idx, row in df.iterrows():
        array_score_differences = np.append(array_score_differences, row['pts_diff'])
        
# Remove NaN values
array_score_differences = array_score_differences[~np.isnan(array_score_differences)]

# Remove all losing scores (deduplicate)
array_score_differences = array_score_differences[array_score_differences >= 0]

# Pandas style
series_score_differences = pd.Series(array_score_differences)
series_score_differences.to_csv(pathlib.Path(dir_path,'score_differences.csv'), index=False)

#------------------------------------------------------------------------------
# Plot the data
ax = series_score_differences.hist(bins=25, grid=False, figsize=(12,8), 
                              color='#86bf91', zorder=2, rwidth=0.9)
#ax = df.hist(column='session_duration_seconds', bins=25, grid=False, 
#             figsize=(12,8), color='#86bf91', zorder=2, rwidth=0.9)

# Despine
ax.spines['right'].set_visible(False)
ax.spines['top'].set_visible(False)
ax.spines['left'].set_visible(False)

# Switch off ticks
ax.tick_params(axis="both", which="both", bottom="off", top="off", 
              labelbottom="on", left="off", right="off", labelleft="on")

# Draw horizontal axis lines
vals = ax.get_yticks()
for tick in vals:
    ax.axhline(y=tick, linestyle='dashed', alpha=0.4, color='#eeeeee', zorder=1)

# Remove title
ax.set_title("")

# Set x-axis label
ax.set_xlabel("Point Margin", labelpad=20, weight='bold', size=12)

# Set y-axis label
ax.set_ylabel("# of Games", labelpad=20, weight='bold', size=12)

# Format y-axis label
ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,g}'))