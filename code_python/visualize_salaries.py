#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 22 14:40:13 2017

@author: ejreidelbach
"""
#------------------------------------------------------------------------------
# Visualize Coaching Salary Data from USA Today's Website
# -----------------------------------------------------------------------------

# Import the Required Libraries
import os
import pandas as pd
import seaborn as sns
import matplotlib as plt

filenames = ['salary_fb_head_team_hist.csv',
             'salary_fb_head_team_coach.csv',
             'salary_fb_asst_hist.csv',
             'salary_fb_str_hist.csv',
             'salary_bb_head_hist.csv']   

###############################################################################
# Function Definitions

def plot_data(dataframe):
    '''
        Function that reads in a dataframe and prints the results
    '''
    lm = sns.lmplot('Conf', 'Total Pay', data=dataframe, fit_reg = False, hue='Conf', scatter_kws={"marker": "D", "s": 25}, size = 10)
    #lm.set(xlim=(8.094,8.098))
    fig = lm.fig
    fig.suptitle('Scatter Plot of RF Trains', fontsize = 18)
    plt.show()   

###############################################################################
# Working Code    
    
# Set the project working directory
os.chdir('/home/ejreidelbach/projects/CollegeFootball/CSVs/Salary')

currentHeadDF = pd.DataFrame.from_csv('current_head.csv', index_col = None)
currentHeadDF.info()
currentHeadDF['Conf'] = currentHeadDF['Conf'].astype('category')


currentHeadDF['Conf'].value_counts().sort_index()
plot_data(currentHeadDF)