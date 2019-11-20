# PURPOSE: Return medians or means of stats to be used for nfl compute median
# Flags:    'qb' 'rb' etc or combinations tell it what positions to run
#           'all' means run all stats
#           'write' means update values that will be used in nflCompute Elo
#           'new' means use the updated scraped data rather than the older R version (nflPlayerGameStats.csv)
# Defaults: every flag is false by default. Must include every argument needed (e.g., `python3 nfl_findStats.py all write new`, `python3 nfl_findStats.py qb rb write`, etc.)
import json
import numpy as np
import pandas as pd
import pickle
import sys
import warnings
from pathlib import Path

from eloUtilities import readplayergamestats

# Get filename from config file
with open(Path("code_python/elo_config.json")) as file:
    cfg = json.load(file)["findStats"]["nfl"]

all_bool = any(x.lower() == "all" for x in sys.argv)
write_bool = any(x.lower() == "write" for x in sys.argv)
new_stats_bool = any(x.lower() == "new" for x in sys.argv)

# Examines the "new" argument. If it exists, use the new scraped stats and combine them. If it does not exist,use the data from previously, the R-code-based one that combines the stats for each year.
if new_stats_bool:
    # This must be run first to give the pickle files access to what they need.
    import nfl_convertPlayerGameStats

    # Make a playerstats file. Read in all seasons and then concatenate them vertically.
    # FIXME: Keeping this set to 2018 for now
    seasons = range(1999, 2018 + 1)
    outlist = []
    for season in seasons:
        playerstatfile = Path(
            "data_raw", "nfl", f"nfl-player-game-statistics{season}.csv")
        playerstats_season = readplayergamestats(playerstatfile)
        outlist.append(playerstats_season)
    playerstats = pd.concat(outlist)
else:
    playerstatfile = Path("data_raw", "nfl", cfg["playerstatfile"])
    playerstats = readplayergamestats(playerstatfile)

if any(x.lower() == "qb" for x in sys.argv) or all_bool:
    # Subset of dataframe
    meet_cond = playerstats[(playerstats["Pos"].isin(["QB"]))].copy()

    # Calculated variables
    # meet_cond["calc_YFS"] = meet_cond["Rush Yard"] + meet_cond["Rec Yards"]
    meet_cond["calc_pass_comp"] = (meet_cond["Pass Comp"] / meet_cond["Pass Att"]) * 100
    meet_cond["calc_yard_per_att"] = meet_cond["Pass Yard"] / meet_cond["Pass Att"]
    meet_cond["calc_yard_per_comp"] = meet_cond["Pass Yard"] / meet_cond["Pass Comp"]

    # Return median for selected variables
    # print("passes:", meet_cond["Pass Att"].median())
    # print("passing Yards:", meet_cond["Pass Yard"].median())
    # print("Pass TD:", meet_cond["Pass TD"].median())
    # print("Pass TD mean:", meet_cond["Pass TD"].mean())
    # print("Pass Int:", meet_cond["Pass Int"].median())
    # print("Pass Int mean:", meet_cond["Pass Int"].mean())
    # print("QBR:", meet_cond["QBR"].median())
    # print("Pass Comp:", meet_cond["calc_pass_comp"].median())
    # print("Yards per Attempt", meet_cond["calc_yard_per_att"].median())
    # print("Yards per Catch", meet_cond["calc_yard_per_comp"].median())
    # print("target:", meet_cond["Targets"].median())

    # saving variables
    stat_dict = {
        "passes": meet_cond["Pass Att"].median(),
        "passDdrawline": meet_cond["Pass Yard"].median(),
        "PassTD": meet_cond["Pass TD"].median(),
        "passTDline": meet_cond["Pass TD"].mean(),
        "PassInt": meet_cond["Pass Int"].median(),
        "passIntline": meet_cond["Pass Int"].mean(),
        "qbrDrawline": meet_cond["QBR"].median(),
        "compPercDrawline": meet_cond["calc_pass_comp"].median(),
        "ydPerAttemptDrawline": meet_cond["calc_yard_per_att"].median(),
        "ydPerCatchDrawline": meet_cond["calc_yard_per_comp"].median(),
    }

    print("QB: ", stat_dict)
    if write_bool:
        pickle.dump(stat_dict, open("data_raw/nfl/qb_stats.pickle", "wb"))
    else:
        pickle.dump(stat_dict, open("data_raw/nfl/qb_stats_temp.pickle", "wb"))


if any(x.lower() == "rb" for x in sys.argv) or all_bool:
    meet_cond = playerstats[
        (playerstats["Pos"].isin(["RB", "HB", "FB"])) & (playerstats["Rush Att"] > 9)
    ].copy()

    # Calculated variables
    meet_cond["calc_YFS"] = meet_cond["Rush Yard"] + meet_cond["Rec Yards"]
    meet_cond["calc_YPC"] = meet_cond["Rush Yard"] / meet_cond["Rush Att"]
    meet_cond["calc_TD"] = meet_cond["Rush TD"] + meet_cond["Rec TD"]

    # Return median for selected variables
    # print("RushAtt", meet_cond["Rush Att"].median())
    # print("RushYPG", meet_cond["Rush Yard"].median())
    # print("YFS", meet_cond["calc_YFS"].median())
    # print("RushYPC", meet_cond["calc_YPC"].median())
    # print("totalTD", meet_cond["calc_TD"].median())
    # print("totalTDmean", meet_cond["calc_TD"].mean())

    stat_dict = {
        "RushAtt": meet_cond["Rush Att"].median(),
        "rushPdrawline": meet_cond["Rush Yard"].median(),
        "yfsPdrawline": meet_cond["calc_YFS"].median(),
        "ydPerCarry": meet_cond["calc_YPC"].median(),
        "totalTD": meet_cond["calc_TD"].median(),
        "TDline": meet_cond["calc_TD"].mean(),
    }

    print("RB: ", stat_dict)
    if write_bool:
        pickle.dump(stat_dict, open("data_raw/nfl/rb_stats.pickle", "wb"))
    else:
        pickle.dump(stat_dict, open("data_raw/nfl/rb_stats_temp.pickle", "wb"))


if any(x.lower() == "wr" for x in sys.argv) or all_bool:
    # Subset of dataframe
    meet_cond = playerstats[playerstats["Pos"].isin(["WR"])].copy()

    # Calculated variables
    meet_cond["calc_rec_yards_0"] = meet_cond["Rec Yards"].fillna(value=0)
    meet_cond["calc_recYardPerTarget"] = (
        meet_cond["calc_rec_yards_0"] / meet_cond["Targets"]
    )
    meet_cond["calc_YPC"] = meet_cond["Rec Yards"] / meet_cond["Rec"]
    meet_cond["calc_recPerTarget"] = (meet_cond["Rec"] / meet_cond["Targets"]) * 100
    meet_cond["calc_ydPerCatch"] = meet_cond["Rec Yards"] / meet_cond["Rec"]

    # Return median for selected variables
    # print("recs:", meet_cond["Rec"].median())
    # print("recYPG:", meet_cond["Rec Yards"].median())
    # print("recYPC:", meet_cond["calc_YPC"].median())
    # print("recTD:", meet_cond["Rec TD"].median())

    stat_dict = {
        "recDrawline": meet_cond["Rec"].median(),
        "recYPG": meet_cond["Rec Yards"].median(),
        "recYPC": meet_cond["calc_YPC"].median(),
        "recTD": meet_cond["Rec TD"].median(),
        "targetDrawline": meet_cond["calc_recPerTarget"].median(),
        "ydPerCatchDrawline": meet_cond["calc_ydPerCatch"].median(),
        "recPdrawline": meet_cond["Rec Yards"].median(),
        "ydPerTargetDrawline": meet_cond["calc_recYardPerTarget"].median(),
        "TDline": meet_cond["Rec TD"].mean(),
    }

    print("WR: ", stat_dict)
    if write_bool:
        pickle.dump(stat_dict, open("data_raw/nfl/wr_stats.pickle", "wb"))
    else:
        pickle.dump(stat_dict, open("data_raw/nfl/wr_stats_temp.pickle", "wb"))


if any(x.lower() == "te" for x in sys.argv) or all_bool:
    meet_cond = playerstats[playerstats["Pos"].isin(["TE", "SE"])].copy()

    # Calculated variables
    meet_cond["calc_rec_yards_0"] = meet_cond["Rec Yards"].fillna(value=0)
    meet_cond["calc_recYardPerTarget"] = (
        meet_cond["calc_rec_yards_0"] / meet_cond["Targets"]
    )
    meet_cond["calc_YPC"] = meet_cond["Rec Yards"] / meet_cond["Rec"]
    meet_cond["calc_recPerTarget"] = meet_cond["Rec"] / meet_cond["Targets"]
    meet_cond["calc_ydPerCatch"] = meet_cond["Rec Yards"] / meet_cond["Rec"]

    # Return median for selected variables
    # print("recs:", meet_cond["Rec"].median())
    # print("recYPG:", meet_cond["Rec Yards"].median())
    # print("recYPC:", meet_cond["calc_YPC"].median())
    # print("recTD:", meet_cond["Rec TD"].median())

    stat_dict = {
        "recDrawline": meet_cond["Rec"].median(),
        "recYPG": meet_cond["Rec Yards"].median(),
        "recYPC": meet_cond["calc_YPC"].median(),
        "recTD": meet_cond["Rec TD"].median(),
        "targetDrawline": meet_cond["calc_recPerTarget"].median(),
        "ydPerCatchDrawline": meet_cond["calc_ydPerCatch"].median(),
        "ydPerTargetDrawline": meet_cond["calc_recYardPerTarget"].median(),
        "recPdrawline": meet_cond["Rec Yards"].median(),
        "TDline": meet_cond["Rec TD"].mean(),
    }
    print("TE: ", stat_dict)
    if write_bool:
        pickle.dump(stat_dict, open(Path("data_raw/nfl/te_stats.pickle"), "wb+"))
    else:
        pickle.dump(
            stat_dict, open(Path("data_raw/nfl/te_stats_temp.pickle"), "wb+")
        )

if len(sys.argv) == 1:
    print("")
    warnings.warn(
        'This script requires "write" as an argument in order to update the values for nflSaveOutput,\nRun with "all" to suppress this warning and calculate all stats. \nRun with "new" to use the new scraped data as source data for drawlines rather than `nflPlayerGameStats.csv`.'
    )

