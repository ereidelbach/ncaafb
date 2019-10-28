#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 24 10:24:49 2018

@author: ejreidelbach

:DESCRIPTION:

:REQUIRES:
   
:TODO:
"""
 
#==============================================================================
# Package Import
#==============================================================================
import os  
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
import csv

#==============================================================================
# Function Definitions / Reference Variable Declaration
#==============================================================================

#==============================================================================
# Working Code
#==============================================================================

# Set the project working directory
os.chdir(r'/home/ejreidelbach/projects/CollegeFootball')

### Coaching Data
df_head_current = pd.read_csv('Data/Salary/current_head.csv')
df_head_past = pd.read_csv('Data/Salary/salary_fb_head_team_coach.csv')

### School records in 2017
df_results_2017 = pd.read_csv('Data/Conf Standings/conf_standings_2017.csv')

### Standardized school names
schoolAbbrevDict = {}
with open('Data/school_abbreviations.csv') as fin:
    reader=csv.reader(fin, skipinitialspace=True, quotechar="'")
    for row in reader:
        valueList = list(filter(None, row[1:]))
        schoolAbbrevDict[row[0]]=valueList

# Standardize schools for head coaches        
list_coach_school = []
for coach_school in df_head_current['School']:
    school_found = False
    for school in schoolAbbrevDict:
        schoolList = [x.lower() for x in schoolAbbrevDict[school]]
        if coach_school.lower() in schoolList:
            list_coach_school.append(school)
            school_found = True
            break
    if school_found == False:
        list_coach_school.append(coach_school)
df_head_current['School'] = list_coach_school
            
# Standardize schools for results
list_results_school = []
for results_school in df_results_2017['School']:
    school_found = False
    for school in schoolAbbrevDict:
        schoolList = [x.lower() for x in schoolAbbrevDict[school]]
        if results_school.lower() in schoolList:
            list_results_school.append(school)
            school_found = True
            break
    if school_found == False:
        list_results_school.append(results_school)
df_results_2017['School'] = list_results_school

### Merge results with salary info
df = pd.merge(df_head_current, df_results_2017, how='outer', on='School')
df.to_csv('Data/Salary/current_head_with_records.csv', index=False)
#pd.set_option('display.float_format', lambda x: '%.3f' % x)
#pd.options.display.float_format = '{:.2f}'.format

# Plt the relationship between salaries and wins
#sns.lmplot(x='Over W', y='Total Pay', data=df, col='Conference', palette='Set1')
sns.barplot(y=df.groupby('Conf').sum()['Total Pay'],
           x='Conf', data = df)
fig, ax = plt.subplots()
ax.ticklabel_format(style='plain')
df.groupby('Conf').sum()['Total Pay'].plot.bar()


# Plot the relationship between salaries and Points Allowed (Def. Coordinators)

# Plot the relationship between salaries and Points Scored (Off. Coordinators)