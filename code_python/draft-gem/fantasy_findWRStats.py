# PURPOSE: Return medians of rushing stats
import json
import numpy as np
from pathlib import Path

from eloUtilities import readplayergamestats

# Get filename from config file
with open(Path("elo_config.json")) as file:
    cfg = json.load(file)["findStats"]["fantasy"]

playerstatfile = Path("..", "data_raw", "fantasy", cfg["playerstatfile"])
print(playerstatfile)

# Read in player stat file
playerstats = readplayergamestats(playerstatfile)

# Subset of dataframe
meet_cond = playerstats[playerstats["Pos"].isin(["WR"])].copy()

# Calculated variables
meet_cond["calc_YFS"] = meet_cond["Rush Yard"] + meet_cond["Rec Yards"]
meet_cond["calc_TD"] = meet_cond["Rush TD"] + meet_cond["Rec TD"]

# Return median for selected variables
print("rec:", meet_cond["Rec"].median())
print("YPG:", meet_cond["Rec Yards"].median())
print("YFS:", meet_cond["calc_YFS"].median())
print("TD:", meet_cond["calc_TD"].median())

# Means
print("YPG mean:", meet_cond["Rec Yards"].mean())
print("rec mean:", meet_cond["Rec"].mean())
print("TD mean:", meet_cond["calc_TD"].mean())
