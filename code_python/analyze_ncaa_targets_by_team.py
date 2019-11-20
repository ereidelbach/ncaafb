import pandas as pd

df = pd.read_csv('/home/ejreidelbach/Projects/draft-analytics-api/src/data/data_scraped/ncaa_player/ncaa_player_2019.csv')

# Count the number of catches per player per team
catches = df.groupby(['School', 'Player', 'position'])['Rec'].sum()

# Drop players with 0 catches
catches = catches[catches > 0]

# split variables out into separate columns
df_subset = pd.DataFrame(catches)
df_subset = df_subset.reset_index()

import ast
df_subset['position_list'] = df_subset['position'].apply(
        lambda x: ast.literal_eval(x))

list_catches_te = []
list_catches_rb = []
list_catches_wr = []
for index, row in df_subset.iterrows():
    if 'TE' in row['position_list']:
        list_catches_te.append(row['Rec'])
        list_catches_rb.append(0)
        list_catches_wr.append(0)
    elif 'RB' in row['position_list']:
        list_catches_rb.append(row['Rec'])
        list_catches_te.append(0)
        list_catches_wr.append(0)
    elif 'WR' in row['position_list']:
        list_catches_wr.append(row['Rec'])
        list_catches_te.append(0)
        list_catches_rb.append(0)
    else:
        list_catches_wr.append(0)
        list_catches_te.append(0)
        list_catches_rb.append(0)        

df_subset['catches_te'] = list_catches_te
df_subset['catches_rb'] = list_catches_rb
df_subset['catches_wr'] = list_catches_wr

df_counts = df_subset.groupby(['School'])['catches_te', 'catches_rb', 'catches_wr'].sum()

df_counts['catches_total'] = df_counts.apply(
        lambda row: row['catches_te'] + row['catches_rb'] + row['catches_wr'],
        axis = 1)
df_counts['% TE'] = df_counts.apply(
        lambda row: row['catches_te'] / row['catches_total'], axis = 1)
df_counts['% RB'] = df_counts.apply(
        lambda row: row['catches_rb'] / row['catches_total'], axis = 1)
df_counts['% WR'] = df_counts.apply(
        lambda row: row['catches_wr'] / row['catches_total'], axis = 1)
df_counts = df_counts.reset_index()

import matplotlib.pyplot as plt
df_counts['% WR'].plot(kind = 'hist', bins = 130, 
         title = "Percentage of Team's Passes Completed to Wide Receivers",
         edgecolor = 'k', alpha = 0.65)
plt.xlabel('% of passes to Wide Receivers')
plt.ylabel('# of Teams')
plt.axvline(float(df_counts[df_counts['School'] == 'Nebraska']['% WR']),
            color = 'red',
            linestyle = 'dashed',
            linewidth = 1)

import matplotlib.pyplot as plt
df_counts['% RB'].plot(kind = 'hist', bins = 100, 
         title = "Percentage of Team's passes Completed to Running Backs")
plt.xlabel('% of passes to Running Backs')
plt.ylabel('# of Teams')
plt.axvline(float(df_counts[df_counts['School'] == 'Nebraska']['% RB']),
            color = 'red',
            linestyle = 'dashed',
            linewidth = 1)

df_counts.to_csv('catches.csv', index = False)
df_teams = pd.read_csv('data_team/teams_ncaa.csv')

df_counts['School'] = rename_school(df_counts, 'School')

df_teams = df_teams.set_index('Team', drop = True)
dict_teams = df_teams.to_dict(orient = 'index')
list_conference = []
for school in list(df_counts['School']):
    list_conference.append(dict_teams[school]['Conference'])
df_counts['Conference'] = list_conference

#----------------------------------------------------------------------
# Isolate wide receivers and run statistical analysis
#----------------------------------------------------------------------
# isolate WRs
df_wr = df_subset[df_subset['position'].str.contains('WR')]

# sort by school and from most to least catches
df_wr = df_wr.sort_values(by = ['School', 'catches_wr'], 
                          ascending = [True, False])
df_wr = df_wr.reset_index(drop = True)
df_wr = df_wr[['School', 'Player', 'position', 'Rec']]

# store total catches per team
catches_school = df_wr.groupby(['School'])['Rec'].sum()

# convert to dictionary for ease of lookup
dict_catches = catches_school.to_dict()

# add % of total to dataframe
df_wr['%_total'] = df_wr.apply(
        lambda row: row['Rec'] / dict_catches[row['School']], axis = 1)

#---------------------------------------------------------------------
# Determine the biggest drop off between a school's #1 and #2
#---------------------------------------------------------------------
list_dropoffs = []
for school in df_wr.groupby('School'):
    df_school = school[1]
    wr_one = df_school.iloc[0]['% Team']
    rec_one = df_school.iloc[0]['Rec']
    wr_two = df_school.iloc[1]['% Team']
    rec_two = df_school.iloc[1]['Rec']
    list_dropoffs.append(
            [school[0], wr_one - wr_two, rec_one - rec_two])
df_dropoffs = pd.DataFrame(list_dropoffs, 
                           columns = ['School', '% diff', '# diff'])
df_dropoffs.to_csv('dropoffs.csv', index = False)

#---------------------------------------------------------------------
# Determine schools whose top receiver has more catches than all others
#   combined (if that exists)
#---------------------------------------------------------------------
list_diff = []
for school in df_wr.groupby('School'):
    df_school = school[1]
    rec_one = df_school.iloc[0]['Rec']
    rec_all = df_school.iloc[1:]['Rec'].sum()
    list_diff.append([school[0], rec_one - rec_all])
df_diff = pd.DataFrame(list_diff, columns = ['School', 'Diff'])
df_diff.to_csv('differences.csv', index = False)