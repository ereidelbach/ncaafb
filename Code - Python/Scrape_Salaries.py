#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 21 13:56:10 2017

@author: ejreidelbach
"""

#------------------------------------------------------------------------------
# Extract Coaching Salary Data from USA Today's Website
# -----------------------------------------------------------------------------

# Import the Required Libraries
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os

# Specify the USA Today Coaching Websites
url_usatoday = ['http://sports.usatoday.com/ncaa/salaries/',
                'http://sports.usatoday.com/ncaa/salaries/mens-basketball/coach',
                'http://sports.usatoday.com/ncaa/salaries/football/assistant',
                'http://sports.usatoday.com/ncaa/salaries/football/strength']

filenames_hist = ['salary_fb_head_team_hist.csv',
                  'salary_fb_head_team_coach.csv',
                  'salary_bb_head_hist.csv',
                  'salary_fb_asst_hist.csv',
                  'salary_fb_str_hist.csv']        

# 11 teams don't have team names (`list_orig`) that match the official NCAA stats page (`list_new`)
list_orig = ["Alabama at Birmingham","Central Florida","Miami (Ohio)",
             "Miami (Fla.)","Mississippi","Nevada-Las Vegas","Southern California",
             "Southern Mississippi","Texas Christian","Texas-El Paso",
             "Texas-San Antonio","Texas AM"]
list_new  = ["UAB","UCF","Miami (OH)","Miami (FL)","Ole Miss","UNLV","USC",
             "Southern Miss","TCU","UTEP","UTSA","Texas A&M"]

###############################################################################
# Function Definitions

def team_info(url):
    '''
        Scrape team information from all universities on file
        
        Input:  
            - url:    string containing the specific URL to be scraped
            
        Ouptput:
            - team_info.csv:  file containing all university-related information
            - Nothing is returned by this function
    '''    
    # Request the site and soupify the data
    r = requests.get(url)
    soup = BeautifulSoup(r.content,'xml')
    
    # Extract the salary data for all recorded years
    table = soup.find('table', class_='datatable datatable-salaries fixed-column')
    rows = table.findAll('tr')
    
    # Create storage variables
    school_dict = {}
    df_team_info = pd.DataFrame()

    # First row will be the column headings, the rest will be coach/school info
    for j in range(len(rows)):
        # For all body rows, extract the school ID ('passid') and the coach ID ('data-coach')
        if j != 0:
            cols = rows[j].findAll('td')
            if (len(cols) > 1):     # Ignore any rows which have no column info
                temp_dict = {}
                temp_dict['schoolID'] = cols[1]['data-passid']
                school_dict[cols[1].text] = temp_dict
                
    # Using the school and coach IDs, extract the historical salary data for every school
    key_count = 0
    for key in school_dict:
        key_count += 1
        
        # Request information for the specific school
        print("Downloading data for #" + str(key_count) + ": " + str(key))
        s = requests.get('http://sports.usatoday.com/ajaxservice/ncaa/salaries__school__' 
            + str(school_dict[key]['schoolID']))

        # Create temporary storage variables
        temp_json = s.json()
        
        # Populate a eparate table with important information concerning each school
        #   Ex:  Team Abbrevation, ID number, Conference, and Logo URL
        team_dict = {}
        team_dict['school'] = key
        team_dict['teamID'] = temp_json['position']
        team_dict['conference'] = temp_json['profile']['conference']
        try:
            team_dict['teamABBR'] = temp_json['profile']['team_abbr']
            team_dict['icon_url'] = ('http://www.gannett-cdn.com/media/SMG/sports_logos/ncaa-whitebg/110/' +
                   str(temp_json['profile']['team_abbr']) + '.png')
        except:
            pass
        temp_df_team = pd.DataFrame([team_dict])
        
        # Append the temporarily created dataframe for the team to the master
        #   dataframe containing info for all teams
        df_team_info = df_team_info.append(temp_df_team)
        
        # Wait 6 seconds before querying the next team so we don't overload the 
        #   the USA Today server and risk getting locked out
        #time.sleep(6)
                
    # Convert the names of select schools to match their format on the NCAA website
    df_team_info[['school']] = df_team_info[['school']].replace(list_orig,list_new)
    
    print("Writing data to: team_info.csv")
    df_team_info.to_csv('team_info.csv', index=False)    

def current_year(url, fname):
    '''
        Scrape salary information for all TEAMS for the most recent year (i.e. current)
        
        Input:  
            - url:    string containing the specific URL to be scraped
            - fname:  string containing the filename (CSV) to which the data should be written
            
        Ouptput:
            - fname.csv:  file containing the year's salary information
            - Nothing is returned by this function
    '''
    # Request the site, soupify the data, and extract the main table (table 0)
    r = requests.get(url)
    soup = BeautifulSoup(r.content,'xml')
    table = soup.find_all('table')[0]

    # Extract the data for the current year    
    df_salary = pd.read_html(str(table))[0]                 # Extract the nested dataframe we want
    df_salary.drop('Rk', axis=1, inplace=True)              # Drop Rank column as we won't need it
    df_salary = df_salary[df_salary.School != 'School']     # Drop footer that made it into the table  
    
    # Convert the salary column to a numeric value
    df_salary[df_salary.columns[3:]] = df_salary[df_salary.columns[3:]].replace(
            '[\$,]','', regex=True).apply(pd.to_numeric, errors='coerce')
    
    # Convert school names to values that match the NCAA website
    df_salary[['School']] = df_salary[['School']].replace(list_orig,list_new)
    
    # Write the current year's salaries to csv files
    df_salary.to_csv(fname, index=False)    

def all_years_team(url, fname):
    '''
        Scrape historical salary information for all TEAMS throughout all available years
        
        Input:  
            - url:    string containing the specific URL to be scraped
            - fname:  string containing the filename (CSV) to which the data should be written
            
        Ouptput:
            - fname.csv:  file containing all historical salary information
            - Nothing is returned by this function
    '''    
    # Request the site and soupify the data
    r = requests.get(url)
    soup = BeautifulSoup(r.content,'xml')
    
    # Extract the salary data for all recorded years
    table = soup.find('table', class_='datatable datatable-salaries fixed-column')
    rows = table.findAll('tr')
    
    # Create storage variables
    salary_dict = {}
    df_salary_hist = pd.DataFrame()
    
    # First row will be the column headings, the rest will be coach/school info
    for j in range(len(rows)):
        # For all body rows, extract the school ID ('passid') and the coach ID ('data-coach')
        if j != 0:
            cols = rows[j].findAll('td')
            if (len(cols) > 1):     # Ignore any rows which have no column info
                temp_dict = {}
                temp_dict['schoolID'] = cols[1]['data-passid']
                temp_dict['coachID'] = cols[1]['data-coach']
                salary_dict[cols[1].text] = temp_dict
                
    # Using the school and coach IDs, extract the historical salary data for every school
    key_count = 0
    for key in salary_dict:
        key_count += 1
        
        # Request information for the specific school
        print("Downloading " + str(fname.split('_')[2]) + " data for #" 
              + str(key_count) + ": " + str(key))
        s = requests.get('http://sports.usatoday.com/ajaxservice/ncaa/salaries__school__' 
            + str(salary_dict[key]['schoolID']) + '__football__' + str(salary_dict[key]['coachID']))

        # Create temporary storage variables
        temp_json = s.json()
        temp_df = pd.DataFrame()         
        
        # For every row of data the school has, extract it and add in the team name
        for k in range(len(temp_json['rows'])):
            temp_df = temp_df.append(pd.DataFrame.from_dict(temp_json['rows'][k]), ignore_index = True)
        temp_df['school'] = key
        temp_df['conference'] = temp_json['profile']['conference']
        
        # Append the temporarily created dataframe to the master salary dataframe
        df_salary_hist = df_salary_hist.append(temp_df)
        
        # Wait 6 seconds before querying the next team so we don't overload the 
        #   the USA Today server and risk getting locked out
        #time.sleep(6)

    # Convert the salary columns from strings to numeric values
    df_salary_hist[df_salary_hist.columns[1:6]] = df_salary_hist[
            df_salary_hist.columns[1:6]].replace('[\$,]','',regex=True).apply(
            pd.to_numeric, errors='coerce')
                
    # Convert missing values to NaN
    df_salary_hist = df_salary_hist.fillna(0)
                
    # Convert the names of select schools to match their format on the NCAA website
    df_salary_hist[['school']] = df_salary_hist[['school']].replace(list_orig,list_new)
        
    # Write the historical salaries to csv files
    print("Writing data to: " + str(fname))
    df_salary_hist.to_csv(fname, index=False)

def all_years_coach(url, fname):
    '''
        Scrape historical salary information for all COACHES throughout all available years
        
        Input:  
            - url:    string containing the specific URL to be scraped
            - fname:  string containing the filename (CSV) to which the data should be written
            
        Ouptput:
            - fname.csv:  file containing all historical salary information
            - Nothing is returned by this function
    '''
    # Request the site and soupify the data
    r = requests.get(url)
    soup = BeautifulSoup(r.content,'xml')
    
    # Extract the salary data for all recorded years
    table = soup.find('table', class_='datatable datatable-salaries fixed-column')
    rows = table.findAll('tr')
    
    # Create storage variables
    salary_dict = {}
    df_coach_info = pd.DataFrame()
    df_salary_hist = pd.DataFrame()
    
    # First row will be the column headings, the rest will be coach/school info
    for j in range(len(rows)):
        # For all body rows, extract the school ID ('passid') and the coach ID ('data-coach')
        if j != 0:
            cols = rows[j].findAll('td')
            if (len(cols) > 1):     # Ignore any rows which have no column info
                salary_dict[cols[3].text] = cols[1]['data-coach']
                
    # Using the school and coach IDs, extract the historical salary data for every school
    key_count = 0
    for key in salary_dict:
        key_count += 1
        
        # Request information for the specific school
        print("Downloading " + str(fname.split('_')[2]) + " data for #" 
              + str(key_count) + ": " + str(key))
        s = requests.get('http://sports.usatoday.com/ajaxservice/ncaa/salaries__coach__' 
            + str(salary_dict[key]))

        # Create temporary storage variables
        temp_json = s.json()
        temp_df = pd.DataFrame()         
        
        # For every row of data the school has, extract it and add in the team name
        for year in temp_json['rows']:
            nested_dict = {'coach':key}
            for cat in year:
                nested_dict[cat] = year[cat]['value']
            temp_df = temp_df.append(pd.DataFrame([nested_dict]))
        
        # Populate a eparate table with important information concerning each school
        #   Ex:  Team Abbrevation, ID number, Conference, and Logo URL
        coach_dict = {}
        coach_dict['coachID'] = temp_json['position']
        coach_dict.update(temp_json['profile'])
        try:
            coach_dict.pop('team_rgb')
        except:
            pass
        try:
            coach_dict['head_shot'] = ('http://www.gannett-cdn.com/media/' +
                   str(coach_dict['head_shot']))
        except:
            pass
        temp_df_coach = pd.DataFrame([coach_dict])
        
        # Append the temporarily created dataframe for the team to the master
        #   dataframe containing info for all teams
        df_salary_hist = df_salary_hist.append(temp_df)
        df_coach_info = df_coach_info.append(temp_df_coach)
        
        # Wait 6 seconds before querying the next team so we don't overload the 
        #   the USA Today server and risk getting locked out
        #time.sleep(6)

    # Convert the salary columns from strings to numeric values
    col_list = ['last_year_bonus', 'max_bonus', 'other_pay', 'school_pay', 'total_pay']               
    df_salary_hist[col_list] = df_salary_hist[col_list].replace(
            '[\$,]','',regex=True).apply(pd.to_numeric, errors='coerce')                
                
    # Convert missing values to NaN
    df_salary_hist = df_salary_hist.fillna(0)
                
    # Convert the names of select schools to match their format on the NCAA website
    df_salary_hist[['school_name']] = df_salary_hist[['school_name']].replace(list_orig,list_new)
    df_coach_info[['school_name']] = df_coach_info[['school_name']].replace(list_orig,list_new)
        
    # Write the historical salaries to a csv file
    print("Writing data to: " + str(fname))
    df_salary_hist.to_csv(fname, index=False)
    
    # Write the information obtained for coaches to a csv file
    print("Writing data to: team_info.csv")
    output_name = "coach_info_" + fname.split('_')[1] + "_" + fname.split('_')[2] + ".csv"
    df_coach_info.to_csv(output_name, index=False)    

###############################################################################
# Working Code
    
# Set the project working directory
os.chdir('/home/ejreidelbach/projects/CollegeFootball/Data/Salary')

# Scrape information for all Universities
team_info(url_usatoday[0])

# Head Football Coach Salaries by University    
all_years_team(url_usatoday[0], filenames_hist[0])

# Head Coach Salaries by Coach
all_years_coach(url_usatoday[0], filenames_hist[1])

# Assistant Coach Salaries by Coach
all_years_coach(url_usatoday[2], filenames_hist[3])

# Strength Coach Salaries by University (only data for 2016)
all_years_team(url_usatoday[3], filenames_hist[4])

df = pd.DataFrame.from_csv(filenames_hist[2])