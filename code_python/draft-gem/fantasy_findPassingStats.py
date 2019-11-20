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
# NOTE: Only getting 2018 + games -- not sure if a test or to be continued
meet_cond = playerstats[
    (playerstats["Pos"].isin(["QB"])) & (playerstats["gamedate"] > 20180000)
].copy()

# Calculated variable
# meet_cond["calc_YFS"] = meet_cond["Rush Yard"] + meet_cond["Rec Yards"]

# Return median for selected variables
print("att:", meet_cond["Pass Att"].median())
print("YPG:", meet_cond["Pass Yard"].median())
print("rushYPG:", meet_cond["Rush Yard"].median())
print("INT:", meet_cond["Pass Int"].median())
print("TD:", meet_cond["Pass TD"].median())
print("rushTD:", meet_cond["Rush TD"].median())

# Means
print("INT mean:", meet_cond["Pass Int"].mean())
print("TD mean:", meet_cond["Pass TD"].mean())
print("rushTD mean:", meet_cond["Rush TD"].mean())
print("INT mean:", meet_cond["Pass Int"].mean())
