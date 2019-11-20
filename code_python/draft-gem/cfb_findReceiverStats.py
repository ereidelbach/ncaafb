# PURPOSE: Return medians of rushing stats
import json
import numpy as np
import pandas as pd
from pathlib import Path

from eloUtilities import readplayergamestats

# Get filename from config file
with open(Path("elo_config.json")) as file:
    cfg = json.load(file)["findStats"]["college"]

extension = ".csv"

# Seasons to run
seasons = range(2005, 2019 + 1)

# Read in files
all_seasons = [
    readplayergamestats(
        Path("..", "data_raw", "cfb", f"{cfg['playerstatfile']}{season}{extension}")
    )
    for season in seasons
]

# Combine all the dataframes into 1 to calculate and find medians for
playerstats = pd.concat(all_seasons, axis=0, ignore_index=True)

# Subset of dataframe
meet_cond = playerstats[playerstats["Rec"] > 0].copy()

# Calculated variables
meet_cond["calc_YPC"] = meet_cond["Rec Yards"] / meet_cond["Rec"]

# Return median for selected variables
print("recs:", meet_cond["Rec"].median())
print("recYPG:", meet_cond["Rec Yards"].median())
print("recYPC:", meet_cond["calc_YPC"].median())
print("recTD:", meet_cond["Rec TD"].median())

# Means
# print("recs:", meet_cond["Rec"].mean())
