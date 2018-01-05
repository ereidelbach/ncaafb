#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  7 13:24:29 2017

@author: ejreidelbach
"""

#------------------------------------------------------------------------------
# Obtain Rankings and Standings Information from the ESPN website
# -----------------------------------------------------------------------------

# Import the Required Libraries
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import re

# 10 teams don't have team names (`list_orig`) that match the official NCAA stats page (`list_new`)
list_orig = ["BYU","Florida Intl","Hawai'i","Louisiana","Louisiana Monroe",
             "Miami","UMass","NC State","SMU","Southern Mississippi","UT San Antonio"]
list_new  = ["Brigham Young","Florida International","Hawaii","Louisiana-Lafayette",
             "Louisiana-Monroe","Miami (FL)","Massachusetts","North Carolina State",
             "Southern Methodist","Southern Miss","UTSA"]

###############################################################################
# Function Definitions

# The following two functions are sourced from for human-sorting purposes:
#   https://stackoverflow.com/questions/5967500/how-to-correctly-sort-a-string-with-a-number-inside
def atof(text):
    try:
        retval = float(text)
    except ValueError:
        retval = text
    return retval

def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    float regex comes from https://stackoverflow.com/a/12643073/190597
    '''
    return [ atof(c) for c in re.split(r'[+-]?([0-9]+(?:[.][0-9]*)?|[.][0-9]+)', text) ]

def site_to_soup(url):
    headers = {"User-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36"}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.content,'html.parser')   
    return soup
    
def conf_standings(year=0):
    ''' This function will scrape the ESPN College Football API
        and return all standings (i.e. wins/losses) for the specified year.  
        Standings returned will be based on the year specified in the input.  
        
        The function will also output a CSV file(s) to the `Conf Standings` folder.
        - each year will result in a separate file
        
        Years are currently limited to 2002 to PRESENT on ESPN.com.
        
        Input:
            year (int) - the year of the season being requested
                * a default value of 0 will return all available years
            
        Output:
            1.A) If the default value of 0 is supplied for year, a dataframe
            containing standings for all historical years on file will be returned.
            
            1.B) If a specific year is supplied, a dataframe containing the standings
            for that specific year will be returned.
            
            2.) A CSV file for each year that is scraped..
    '''  
    # Scrape the main rankings page to establish links for specific weeks/years
    soup = site_to_soup("http://www.espn.com/college-football/standings")
    
    # Grab Year Information
    menu = soup.findAll('ul', class_='dropdown-menu med')[1]
    years_list = []
    years = menu.select('li')
    for yr in years:
        years_list.append(yr.text)

    
    # If the user did not enter or a number or entered 0, grab all years. 
    #   Otherwise, confirm the year that the user entered is available for 
    #   scraping and then only grab that year.
    if str(year) == '0':
        pass
    elif str(year) in years_list:
        years_list = [str(year)]
    else:
        raise ValueError("Year entered is not in the range for ESPN's site.")

    dfStandingsAll = pd.DataFrame()
        
    # Grab Standing Information for the specified year(s)
    for year in years_list:
        print("Processing Rankings for Year: " + str(year))
        soup = site_to_soup("http://www.espn.com/college-football/standings/_/season/" + str(year))

        dfStandingsYear = pd.DataFrame()

        # Grab the column header information
        column_names = []
        column_names.append('Conference')   # Manually set the first item
        column_names.append('Division')     # Manually set the second item
        column_names.append('School')         # Manually set the third item
        headings = soup.findAll('thead', {'class':'standings-categories'})[1]
        cols = headings.findAll('span')[1:] # We don't need the division name so start at 1
        for col in cols:
            column_names.append(col.text)
        column_names.append('Year')
        
        # Add Conf to the front of the Conference PF and PA columns
        column_names[4] = 'Conf PF'
        column_names[5] = 'Conf PA'
        
        # Add Over to the front of the Overall PF and PA columns
        column_names[7] = 'Over PF'
        column_names[8] = 'Over PA'
        
        # Grab all the conference names
        conferences = soup.findAll('h2', class_='table-caption')
                
        # Grab all the divisions within each conference
        standingsAll = soup.findAll('table', {'class':'standings has-team-logos'})

        conf_count = 0
        
        # Loop over each division in every conference and extract all ranking info       
        for division in standingsAll:  
            # setup storage variables
            division_list = []
            division_name = ''
            
            # move through each team in the division
            for row in division:
                team_list = []

                # Ignore rows that don't contain team/header information
                if len(row) > 11:
                    # Grab the name of the division we're scraping              
                    if row['class'][0] == 'standings-categories':
                        cols = row.findAll('span')
                        division_name = cols[0].text
    
                    # Collect team information for each team in the division
                    elif row['class'][0] == '':
                        # Add in conference name
                        team_list.append(conferences[conf_count].text)
                        
                        # Add in division name
                        team_list.append(division_name)
                        
                        # Extract team name
                        team_list.append(row.find('span', class_='team-names').text)
                        
                        # Pull stat information
                        team_cols = row.findAll('td', {'style':'white-space:no-wrap;'})
                        for team_col in team_cols:
                            team_list.append(team_col.text)
                        
                        # Add in the year we're scraping
                        team_list.append(str(year))
                
                # If we scraped a team's row, add it to the division list
                if len(team_list) > 0:
                    division_list.append(team_list)
            
            # Add the division list to the overall standings dataframe
            dfStandingsYear = dfStandingsYear.append(pd.DataFrame(division_list), ignore_index=False)
            
            # Iterate to the next conference in the list
            conf_count += 1   
            
        # Once the dataframe has been created, add in column-names    
        dfStandingsYear.columns = column_names
    
        # Split the Division names such that we only retain the actual name
        #   (i.e. `East` or `West` vice `Big Ten - East` or `Big Ten - West`)
        dfStandingsYear.Division = dfStandingsYear.Division.str.split(' - ').apply(lambda x: x[0] if len(x) == 1 else x[1])
        
        # Remove the word `Conference` from Conference names
        dfStandingsYear.Conference = dfStandingsYear.Conference.str.replace(' Conference', '')
        
        # Split Conference Records into separate `Wins` and `Losses` columns
        dfStandingsYear[['Conf W','Conf L']] = dfStandingsYear.CONF.str.split('-', expand=True, n=1)
        
        # Split Overall Records into separate `Wins` and `Losses` columns
        dfStandingsYear[['Over W','Over L']] = dfStandingsYear.OVER.str.split('-', expand=True, n=1)
        
        # Split Home Records into separate `Wins` and `Losses` columns
        dfStandingsYear[['Home W','Home L']] = dfStandingsYear.HOME.str.split('-', expand=True, n=1)
        
        # Split Away Records into separate `Wins` and `Losses` columns
        dfStandingsYear[['Away W','Away L']] = dfStandingsYear.AWAY.str.split('-', expand=True, n=1)
        
        # Split Records vs AP Top 25 into separate `Wins` and `Losses` columns
        dfStandingsYear[['AP W', 'AP L']] = dfStandingsYear.AP.str.split('-', expand=True, n=1)
        
        # Split Records vs USA Top 25 into separate `Wins` and `Losses` columns
        dfStandingsYear[['USA W', 'USA L']] = dfStandingsYear.USA.str.split('-', expand=True, n=1)
        
        # Reorder all the columns in a preferable manner
        dfStandingsYear = dfStandingsYear[['Conference','Division','School','Conf W','Conf L','Conf PF',
                     'Conf PA','Over W','Over L','Over PF','Over PA','Home W',
                     'Home L','Away W','Away L','STRK','AP W','AP L','USA W','USA L','Year']]
        
        # Write Conference Standings to a CSV file
        filename = 'Conf Standings/conf_standings_' + str(year) + '.csv'
        dfStandingsYear.to_csv(filename, index=False)   
        
        # Append current year rankings to master dfStandings dataframe
        dfStandingsAll = dfStandingsAll.append(dfStandingsYear)
            
    # Write CSV containing all Years
    dfStandingsAll.to_csv('Conf Standings/conf_standings_ALL.csv', index=False)    
    return dfStandingsAll

def poll_rankings(year=0):
    ''' This function will scrape the ESPN College Football API
        and return all rankings for the specified year.  Rankings returned will
        be based on the year specified in the input.  
        
        The function will also output a CSV file(s) to the `Poll Rankings` folder.
        - each year will result in a separate file
        
        Years are currently limited to 2002 to PRESENT on ESPN.com.
        
        Input:
            year (int) - the year of the season being requested
                * the default value of 0 will request all available years
            
        Output:
            1.) A dictionary which contains the ranking for the specifc year. 
            Exact rankings that will be returned depend on the year requested:
                - 2002 to 2006:
                    * AP Poll
                - 2007 to 2013:
                    * AP Poll, BCS Rankings***
                    *** BCS Rankings are only available beginning in week 9
                - 2014 to PRESENT:
                    * AP Poll, CFP Rankings***
                    *** The CFP Rankings are only available beginning in week 10
                    
            2.) A CSV file for each year that is scraped containing all the data 
            in each dictionary and is then exported to the Data/Poll Rankings folder.
    '''

    # Scrape the main rankings page to establish links for specific weeks/years
    soup = site_to_soup("http://www.espn.com/college-football/rankings")
    
    # Grab Year Information
    menu = soup.find('ul', class_='dropdown-menu med')
    years_list = []
    years = menu.select('li')
    for li in years:
        years_list.append(li.text)
      
    # Determine what years should be scraped:
    #   If the user did not enter or a number or entered 0, grab all years. 
    #   Otherwise, confirm the year that the user entered is available for 
    #   scraping and then only grab that year.
    if str(year) == '0':
        pass
    elif str(year) in years_list:
        years_list = [str(year)]
    else:
        raise ValueError("Year entered is not in the range for ESPN's site.")

    # We're only interested in the AP, BCS and College Football Playoff polls
    #   so we'll make a list to compare to when extracting polls
    desired_polls = {'AP Top 25':'AP', 'BCS Standings':'BCS', 'College Football Playoff Rankings':'CFP'}

    rankings_dict = {}
        
    # Grab Standing Information for the specified year(s)
    for year in years_list:
        print("Processing Rankings for Year: " + str(year))    
    
        # Grab Week Information for the specified year
        soup = site_to_soup("http://www.espn.com/college-football/rankings/_/week/1/year/" + str(year) + "/seasontype/2")
        
        # Determine what weeks are available for a given year
        menu = soup.find('ul', class_='dropdown-menu display-desktop med')
        weeks = menu.select('li')
        week_list = []
        for li in weeks:
            week_list.append(li.text)

        # Create a list of links for all possible weeks in a given year
        week_links = {}
        links = menu.find_all('a', href=True)
        for wk, link in zip(weeks, links):
            week_links[wk.text] = "http://www.espn.com" + link['href']

#        # Determine which weeks should be scraped:
#        #   If the user did not enter or a number or entered 0, grab all weeks. 
#        #   Otherwise, confirm the week that the user entered is available for 
#        #   scraping in the current year and then only grab that week.
#        if str(week) == '0':
#            pass
#        elif week in range(week_list):
#            true_index = week_list.index(str(week))
#            week_links = week_links[true_index]
#        else:
#            raise ValueError("Week entered is not in " + str(year) + "'s range for ESPN's site.") 
    
        # Grab the rankings for every week of the year
        year_dict = {}
        ap_df = pd.DataFrame({'Ranking':range(1,26)})
        bcs_df = pd.DataFrame({'Ranking':range(1,26)})
        cfp_df = pd.DataFrame({'Ranking':range(1,26)})
        for week, link in week_links.iteritems():
            print("Grabbing Year " + str(year) + ", " + str(week))
            week_dict = {}
            
            poll_type = []
            
            # Grab Week Information for the specified year and determine what 
            #   poll(s) are available for the week
            soup = site_to_soup(link)
            polls = soup.findAll('h2', class_='table-caption')
            for poll in polls:
                poll_type.append(poll.text)
            
            tables = soup.findAll('table', class_='rankings has-team-logos')
            for k in range(len(tables)):
                rows = tables[k].findAll('tr')
                
                # For every poll in the week, grab the rankings
                poll_list = []
                for i in range(1,len(rows)):
                    try:
                        poll_list.append(rows[i].findAll('td')[1].findAll('span')[0].text)
                    except:
                        pass
#                    row_list = []
#                    # If the first row, pull out the column headings (i.e. poll columns)
#                    if i == 0:
#                        cols = rows[i].findAll('th')
#                        for th in cols:
#                            row_list.append(th.text)
#                    # If any other row, pull out the team's information
#                    else:
#                        cols = rows[i].findAll('td')
#                        for j in range(0, len(cols)):
#                            if j == 1:
#                                row_list.append(cols[j].findAll('span')[0].text)
#                            else:
#                                row_list.append(cols[j].text)
#                    
#                    # Ensure that ESPN doesn't add any unnecessary rows to the master list
#                    #   (i.e. rows of length 1)
#                    if len(row_list) > 2:
#                        poll_list.append(row_list)
                
                if poll_type[k] in desired_polls.keys():
                    week_dict[poll_type[k]] = poll_list
                
            year_dict[week] = week_dict
        
            # Turn year dict into dataframes for each type of ranking
            for week in year_dict:
                # AP Rankings
                try:
                    if len(year_dict[week]['AP Top 25']) > 0:
                        ap_df[week] = year_dict[week]['AP Top 25']
                except:
                    pass
                    
                # BCS Rankings
                try:
                    if len(year_dict[week]['BCS Standings']) > 0:
                        bcs_df[week] = year_dict[week]['BCS Standings']
                except:
                    pass
                
                # College Footbal Playoff Rankings
                try:
                    if len(year_dict[week]['College Football Playoff Rankings']) > 0:
                        cfp_df[week] = year_dict[week]['College Football Playoff Rankings']
                except:
                    pass
            
        # Reorder the rankings columns into a correct order then export a CSV
        # Use Human-sorting to place the weeks in the correct order, move `Rankings`
        #   to be the first column, and move `Final Rankings` to be the last column
        
        # AP
        if len(ap_df.columns) > 1:
            tmp_list = ap_df.columns.tolist()
            tmp_list.sort(key = natural_keys)
            tmp_list.insert(0, tmp_list.pop(tmp_list.index('Ranking')))
            if 'Final Rankings' in tmp_list:
                tmp_list.insert(len(tmp_list)-1, tmp_list.pop(tmp_list.index('Final Rankings')))
            ap_df = ap_df[tmp_list]
            ap_df.to_csv('Poll Rankings/' + str(year) + '_ap.csv', index=False)
        
        # BCS
        if len(bcs_df.columns) > 1:
            tmp_list = list(bcs_df.columns.values)
            tmp_list.sort(key = natural_keys)
            tmp_list.insert(0, tmp_list.pop(tmp_list.index('Ranking')))
            if 'Final Rankings' in tmp_list:
                tmp_list.insert(len(tmp_list)-1, tmp_list.pop(tmp_list.index('Final Rankings')))            
            bcs_df = bcs_df[tmp_list]
            bcs_df.to_csv('Poll Rankings/' + str(year) + '_bcs.csv', index=False)
        
        # CFP
        if len(cfp_df.columns) > 1:
            tmp_list = list(cfp_df.columns.values)
            tmp_list.sort(key = natural_keys)
            tmp_list.insert(0, tmp_list.pop(tmp_list.index('Ranking')))
            if 'Final Rankings' in tmp_list:
                tmp_list.insert(len(tmp_list)-1, tmp_list.pop(tmp_list.index('Final Rankings')))
            cfp_df = cfp_df[tmp_list]
            cfp_df.to_csv('Poll Rankings/' + str(year) + '_cfp.csv', index=False)
            
        rankings_dict[year] = year_dict
            
    return rankings_dict 
    

###############################################################################
# Working Code

# Set the project working directory
os.chdir(r"C:\MSA\Projects\ncaafb\Data")

# Read in Conference Standings for all available years (and write them to CSV)
ts = conf_standings()

# Read in Poll Rankings for all available years (and write them to CSV)
tr = poll_rankings()