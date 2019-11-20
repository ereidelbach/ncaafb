#!/usr/bin/env python3
# PURPOSE: process passing and rushing statistics for first few seasons for the teams, to serve as initialized values that player Elo ratings build off from.
import json
import pandas as pd
from copy import deepcopy
from pathlib import Path

from eloUtilities import get_start_date, readteamgamedata, updateElo
from cfb_getPowerFive import power5teams

# Run the code to convert stats needed for runOldSeasons
#import cfb_convertPlayerGameStats as convertPlayerGameStats
#import cfb_convertTeamStats as convertTeamStats


# SECTION: Configuration
LEAGUE = "college"
BOOST_POWER_5 = True
blowoutFactor = True

with open(Path("code_python/elo_config.json")) as file:
    cfg = json.load(file)

# To determine how to read in the teamgame files
nfl = True
if LEAGUE == "college":
    config = cfg["run_old_seasons"]["college"]
    nfl = False
elif LEAGUE == "nfl":
    config = cfg["run_old_seasons"]["nfl"]
elif LEAGUE == "fantasy":
    config = cfg["run_old_seasons"]["fantasy"]


# SECTION: Setup of default dictionaries
# Starter values
initialEloList = [config["initialTeamElo"]]
initialDateList = [config["initialDate"]]
initialOppList = [0]

teams_default = {
    "last": config["initialTeamElo"],
    "elo": initialEloList,
    "date": initialDateList,
    "opp": initialOppList,
}


# SECTION: Process games for rushing and passing
def run_games(
    seasons,
    blowoutFactor,
    teamK,
    opp_perf_var,
    drawline,
    yardFactor,
    BOOST_POWER_5=False,
    power5teams=[],
    power5initialTeamElo=0,
):
    # Dictionaries for defense and offense
    D_dicts = {}
    O_dicts = {}

    # Filename templates
    teamstatroot = "ncaa-team-game-statistics"
    extension = ".csv"

    for season in seasons:
        # Statistics for each game
#        teamgamefile = Path(
#            "..", "data_raw", "cfb", f"{teamstatroot}{season}{extension}"
#        )
        teamgamefile = Path(
            "data_raw", "cfb", f"{teamstatroot}{season}{extension}"
        )
        teamstats = readteamgamedata(teamgamefile, nfl=nfl)

        # Loop over each date within the season - update Elo after each game
        dates = list(teamstats["gamedate"].unique())

        for date in dates:
            # List of the date's games (both teams' perspectives)
            teams = teamstats[teamstats["gamedate"] == date].to_dict("records")

            # Evaluate team rush D and rush O (overall)
            for row in teams:
                # Codes for the defensive and offensive teams
                us_id = row["Team Code"]
                them_id = row["Team Code opp"]

                # Initialize team dictionary if not already present
                if us_id not in D_dicts:
                    D_dicts[us_id] = deepcopy(teams_default)
                    D_dicts[us_id]["Team Code"] = us_id
                    if BOOST_POWER_5 and us_id in power5teams:
                        D_dicts[us_id]["elo"][0] = power5initialTeamElo
                        D_dicts[us_id]["last"] = power5initialTeamElo

                if them_id not in O_dicts:
                    O_dicts[them_id] = deepcopy(teams_default)
                    O_dicts[them_id]["Team Code"] = them_id
                    if BOOST_POWER_5 and them_id in power5teams:
                        O_dicts[them_id]["elo"][0] = power5initialTeamElo
                        O_dicts[them_id]["last"] = power5initialTeamElo

                # Get teams' latest Elo ratings
                lastDelo = D_dicts[us_id]["elo"][-1]
                lastOelo = O_dicts[them_id]["elo"][-1]

                # Team performance based on yards per game
                # Calculate point differential - in this case yards
                opperf = row[opp_perf_var]
                ptdiff = abs(opperf - drawline) / yardFactor
                win = opperf < drawline

                dElo, oElo = updateElo(
                    lastDelo, lastOelo, ptdiff, win, blowoutFactor, teamK, both=True
                )

                # Update last Elo rating
                D_dicts[us_id]["last"] = dElo
                O_dicts[them_id]["last"] = oElo

                # Update Elo array
                D_dicts[us_id]["elo"] = D_dicts[us_id]["elo"] + [dElo]
                O_dicts[them_id]["elo"] = O_dicts[them_id]["elo"] + [oElo]

                # Update opponents array
                D_dicts[us_id]["opp"] = D_dicts[us_id]["opp"] + [them_id]
                O_dicts[them_id]["opp"] = O_dicts[them_id]["opp"] + [us_id]

                # Update dates

                # Add the date (if first time, add a date for the first date)
                if len(D_dicts[us_id]["date"]) == 1:
                    D_dicts[us_id]["date"][0] = get_start_date(date)
                if len(O_dicts[them_id]["date"]) == 1:
                    O_dicts[them_id]["date"][0] = get_start_date(date)
                D_dicts[us_id]["date"] = D_dicts[us_id]["date"] + [date]
                O_dicts[them_id]["date"] = O_dicts[them_id]["date"] + [date]

    # Completed dictionaries
    return D_dicts, O_dicts

def set_baselines():
    # Make and save dataframes
    rushD, rushO = run_games(
        range(2000, 2004 + 1),
        blowoutFactor,
        config["teamK"],
        config["rushing"]["opp_perf_var"],
        config["rushing"]["drawline"],
        config["rushing"]["yardFactor"],
        BOOST_POWER_5,
        power5teams,
        config["power5initialTeamElo"],
    )
    
    passD, passO = run_games(
        range(2000, 2004 + 1),
        blowoutFactor,
        config["teamK"],
        config["passing"]["opp_perf_var"],
        config["passing"]["drawline"],
        config["passing"]["yardFactor"],
        BOOST_POWER_5,
        power5teams,
        config["power5initialTeamElo"],
    )
    
    return rushD, rushO, passD, passO
