#!/usr/bin/env python3
# PURPOSE: process passing and rushing statistics for first few seasons for the teams, to serve as initialized values that player Elo ratings build off from.
import json
import pandas as pd
from copy import deepcopy
from math import log, log10
from pathlib import Path

import eloUtilities as eu

# Run the code to convert stats needed for runOldSeasons
import fantasy_convertPlayerGameStats
import fantasy_convertTeamStats

# SECTION: Configuration
LEAGUE = "fantasy"
BOOST_POWER_5 = True
blowoutFactor = True
regress = True
addRegressionDataPoint = True

with open(Path("elo_config.json")) as file:
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
    "lastTD": config["initialTeamElo"],
    "eloTD": initialEloList,
    "date": initialDateList,
    "opp": initialOppList,
}


# SECTION: Process games for rushing and passing
def run_games(
    seasons,
    blowoutFactor,
    teamK,
    opp_perf_var,
    opp_perf_var2,
    drawline,
    tdline,
    nTeams,
    regress=True,
    addRegressionDataPoint=True,
):
    # Filename templates
    teamstatroot = "fantasy-team-game-statistics"
    extension = ".csv"

    # Make the "big" teamstats file
    outlist = []
    for season in seasons:
        # Statistics for each game
        teamgamefile = Path(
            "..", "data_raw", "fantasy", f"{teamstatroot}{season}{extension}"
        )
        teamstats_season = eu.readteamgamedata(teamgamefile, nfl=nfl)
        outlist.append(teamstats_season)
    teamstats = pd.concat(outlist)

    # Team Numbers to use
    teamnum = list(range(1, nTeams + 1))

    # Dictionaries for defense and offense
    # NOTE: because of the regression, initializing all teams.
    D_dicts = {x: deepcopy(teams_default) for x in teamnum}
    for team in D_dicts:
        D_dicts[team]["Team Code"] = team
    O_dicts = {x: deepcopy(teams_default) for x in teamnum}
    for team in O_dicts:
        O_dicts[team]["Team Code"] = team

    # To make sure the date is appropriately initialized
    season = 19940820
    # Loop over each date within the season - update Elo after each game
    dates = list(teamstats["gamedate"].unique())

    for date in dates:
        # Regress team Elo on season change
        if (date - season) > 9500:
            season = date

            if regress:
                # Compute average elo
                avgDelo = 0
                avgOelo = 0
                avgDeloTD = 0
                avgOeloTD = 0
                teamCount = 0

                # Get sums of different elo values
                for tm in teamnum:
                    # Make sure team exists and played previous season
                    if len(D_dicts[tm]["elo"]) > 1:
                        avgDelo = avgDelo + D_dicts[tm]["elo"][-1]
                        avgOelo = avgOelo + O_dicts[tm]["elo"][-1]
                        avgDeloTD = avgDeloTD + D_dicts[tm]["elo"][-1]
                        avgOeloTD = avgOeloTD + O_dicts[tm]["elo"][-1]
                        teamCount += 1
                avgDelo = round(avgDelo / teamCount)
                avgOelo = round(avgOelo / teamCount)
                avgDeloTD = round(avgDeloTD / teamCount)
                avgOeloTD = round(avgOeloTD / teamCount)

                # Regress team Elo based on avg
                for tm in teamnum:
                    # Make sure team exists and played previous season
                    if len(D_dicts[tm]["elo"]) > 1:
                        lastElo = D_dicts[tm]["elo"][-1]
                        newElo = round(0.75 * lastElo + 0.25 * avgDelo)
                        if addRegressionDataPoint:
                            D_dicts[tm]["elo"] = D_dicts[tm]["elo"] + [newElo]
                        else:  # otherwise, replace the last one
                            D_dicts[tm]["elo"][-1] = newElo

                        lastElo = D_dicts[tm]["eloTD"][-1]
                        newElo = round(0.75 * lastElo + 0.25 * avgDeloTD)
                        if addRegressionDataPoint:
                            D_dicts[tm]["eloTD"] = D_dicts[tm]["eloTD"] + [newElo]
                        else:
                            D_dicts[tm]["eloTD"][-1] = newElo

                        lastElo = O_dicts[tm]["elo"][-1]
                        newElo = round(0.75 * lastElo + 0.25 * avgOelo)
                        if addRegressionDataPoint:
                            O_dicts[tm]["elo"] = O_dicts[tm]["elo"] + [newElo]
                        else:
                            O_dicts[tm]["elo"][-1] = newElo

                        lastElo = O_dicts[tm]["eloTD"][-1]
                        newElo = round(0.75 * lastElo + 0.25 * avgOeloTD)
                        if addRegressionDataPoint:
                            O_dicts[tm]["eloTD"] = O_dicts[tm]["eloTD"] + [newElo]
                        else:
                            O_dicts[tm]["eloTD"][-1] = newElo

                        if addRegressionDataPoint:
                            # Align other arrays with the regression
                            date = D_dicts[tm]["date"][-1]
                            D_dicts[tm]["date"] = D_dicts[tm]["date"] + [date]
                            D_dicts[tm]["opp"] = D_dicts[tm]["opp"] + [0]
                            date = O_dicts[tm]["date"][-1]
                            O_dicts[tm]["date"] = O_dicts[tm]["date"] + [date]
                            O_dicts[tm]["opp"] = O_dicts[tm]["opp"] + [0]

            ### END REGRESS

        # List of the date's games (both teams' perspectives)
        teams = teamstats[teamstats["gamedate"] == date].to_dict("records")

        # Evaluate team rush D and rush O (overall)
        for row in teams:
            # Codes for the defensive and offensive teams
            us_id = row["Team Code"]
            them_id = row["Team Code opp"]

            # Get the latest Elo rating the two teams have
            lastDelo = D_dicts[us_id]["elo"][-1]
            lastOelo = O_dicts[them_id]["elo"][-1]

            # Team performance based on yards per game
            # Calculate point differential - in this case yards
            opperf = row[opp_perf_var]
            ptdiff = abs(opperf - drawline)
            win = opperf < drawline

            dElo, oElo = eu.updateElo(
                lastDelo, lastOelo, ptdiff, win, blowoutFactor, teamK, both=True
            )

            # Update opponents array
            D_dicts[us_id]["opp"] = D_dicts[us_id]["opp"] + [them_id]
            O_dicts[them_id]["opp"] = O_dicts[them_id]["opp"] + [us_id]

            # Update dates, Elo, and last Elo
            D_dicts[us_id]["date"] = D_dicts[us_id]["date"] + [date]
            O_dicts[them_id]["date"] = O_dicts[them_id]["date"] + [date]

            D_dicts[us_id]["elo"] = D_dicts[us_id]["elo"] + [dElo]
            O_dicts[them_id]["elo"] = O_dicts[them_id]["elo"] + [oElo]

            D_dicts[us_id]["last"] = dElo
            O_dicts[them_id]["last"] = oElo

            # Get the latest EloTD rating the two teams have
            lastDeloTD = D_dicts[us_id]["eloTD"][-1]
            lastOeloTD = O_dicts[them_id]["eloTD"][-1]

            # Team performance based on yards per game
            # Calculate point differential - in this case yards
            opperf = row[opp_perf_var2]
            ptdiff = abs(opperf - tdline)
            win = opperf < tdline

            dEloTD, oEloTD = eu.updateElo(
                lastDeloTD, lastOeloTD, ptdiff, win, blowoutFactor, teamK, both=True
            )

            # Update EloTD and last EloTD
            D_dicts[us_id]["eloTD"] = D_dicts[us_id]["eloTD"] + [dEloTD]
            O_dicts[them_id]["eloTD"] = O_dicts[them_id]["eloTD"] + [oEloTD]

            D_dicts[us_id]["lastTD"] = dEloTD
            O_dicts[them_id]["lastTD"] = oEloTD

    # Completed datasets
    return D_dicts, O_dicts


# Make and save dataframes
rushD, rushO = run_games(
    range(1994, 1998 + 1),
    blowoutFactor,
    config["teamK"],
    config["rushing"]["opp_perf_var"],
    config["rushing"]["opp_perf_var2"],
    config["rushing"]["drawline"],
    config["rushing"]["tdline"],
    config["nTeams"],
)

passD, passO = run_games(
    range(1994, 1998 + 1),
    blowoutFactor,
    config["teamK"],
    config["passing"]["opp_perf_var"],
    config["passing"]["opp_perf_var2"],
    config["passing"]["drawline"],
    config["passing"]["tdline"],
    config["nTeams"],
)
