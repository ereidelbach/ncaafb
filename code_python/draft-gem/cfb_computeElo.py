#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# PURPOSE: Capture operations for processing Elo
import json
import pandas as pd
import numpy as np
from copy import deepcopy
from math import log, log10
from pathlib import Path

import eloUtilities as eu
#from code_python.cfb_runOldSeasons import rushD, rushO, passD, passO
import cfb_runOldSeasons
from cfb_getPowerFive import power5teams

rushD, rushO, passD, passO = cfb_runOldSeasons.set_baselines()

with open(Path("code_python/elo_config.json")) as file:
    cfg = json.load(file)["computeElo"]["college"]

# Groups to process
processRBs = True
processWRs = True
processQBs = True
processDefense = True

blowoutFactor = True
BOOST_POWER_5 = True

# Configuration
initialPlayerElo = 1300
initialTeamElo = 1200
power5InitialTeamElo = 1300
playerK = 20
teamK = 20

rushDdrawline = 150
rushPdrawline = rushDdrawline * 0.5564516
yfsPdrawline = 111
passDdrawline = 218
qbrDrawline = 125.8
recPdrawline = 39
TDline = 0.1
ydPerCarry = 4.0
yardFactor = 1
ypcFactor = 5
fumline = 0.1

passdrawline = 218
compPercDrawline = 59
passTDline = 1.0
passIntline = 0.975
recDrawline = 2
ydPerAttemptDrawline = 7.0
ydPerCatchDrawline = 9.5
passYardFactor = 8
passYPCFactor = 5
pctFactor = 2
recFactor = 3

rushTeamDrawline = 150
passTeamDrawline = 218
tackleDrawline = 3.0
tflDrawline = 0.3
sackDrawline = 0.05
intDrawline = 0.05
ffDrawline = 0.05
pbuDrawline = 0.5
tdFactor = 5
intFactor = 5

seasons = range(2003, 2019 + 1)

# Starter values
initialEloList = [initialPlayerElo]
initialDateList = [0]
initialOppList = [0]

# Holding and default dictionaries for each type of player
if processRBs:
    rushers = {}
    # Initialize Rushing Elo data
    rushers_default = {
        "wYPG": 0,
        "lYPG": 0,
        "wYPC": 0,
        "lYPC": 0,
        "wYFS": 0,
        "lYFS": 0,
        "wTD": 0,
        "lTD": 0,
        "last": initialPlayerElo,
        "lastYPG": initialPlayerElo,
        "lastYPC": initialPlayerElo,
        "lastYFS": initialPlayerElo,
        "lastTD": initialPlayerElo,
        "count": 0,
        "ypgElo": initialEloList,
        "ypcElo": initialEloList,
        "yfsElo": initialEloList,
        "tdElo": initialEloList,
        "eloC": initialEloList,
        "date": initialDateList,
        "opp": initialOppList,
    }

if processWRs:
    receivers = {}
    # Initialize Receiving Elo data
    receivers_default = {
        "wRec": 0,
        "lRec": 0,
        "wYPG": 0,
        "lYPG": 0,
        "wYPC": 0,
        "lYPC": 0,
        "wTD": 0,
        "lTD": 0,
        "last": initialPlayerElo,
        "lastRec": initialPlayerElo,
        "lastYPG": initialPlayerElo,
        "lastYPC": initialPlayerElo,
        "lastTD": initialPlayerElo,
        "count": 0,
        "recElo": initialEloList,
        "ypgElo": initialEloList,
        "ypcElo": initialEloList,
        "tdElo": initialEloList,
        "eloC": initialEloList,
        "date": initialDateList,
        "opp": initialOppList,
    }

if processQBs:
    passers = {}
    # Initialize Passing ELO data
    passers_default = {
        "wQBR": 0,
        "lQBR": 0,
        "wYPG": 0,
        "lYPG": 0,
        "wYPC": 0,
        "lYPC": 0,
        "wYPA": 0,
        "lYPA": 0,
        "wPCT": 0,
        "lPCT": 0,
        "wTD": 0,
        "lTD": 0,
        "wINT": 0,
        "lINT": 0,
        "last": initialPlayerElo,
        "lastQBR": initialPlayerElo,
        "lastYPG": initialPlayerElo,
        "lastYPC": initialPlayerElo,
        "lastYPA": initialPlayerElo,
        "lastPct": initialPlayerElo,
        "lastTD": initialPlayerElo,
        "lastInt": initialPlayerElo,
        "count": 0,
        "qbrElo": initialEloList,
        "ypgElo": initialEloList,
        "ypcElo": initialEloList,
        "ypaElo": initialEloList,
        "pctElo": initialEloList,
        "tdElo": initialEloList,
        "intElo": initialEloList,
        "eloC": initialEloList,
        "date": initialDateList,
        "opp": initialOppList,
    }

if processDefense:
    defense = {}
    # Initialize Defense Elo data
    defense_default = {
        "wTackles": 0,
        "lTackles": 0,
        "wTFL": 0,
        "lTFL": 0,
        "wSack": 0,
        "lSack": 0,
        "wINT": 0,
        "lINT": 0,
        "wPBU": 0,
        "lPBU": 0,
        "wFF": 0,
        "lFF": 0,
        "lastTackles": initialPlayerElo,
        "lastTFL": initialPlayerElo,
        "lastSack": initialPlayerElo,
        "lastInt": initialPlayerElo,
        "lastPBU": initialPlayerElo,
        "lastFF": initialPlayerElo,
        "count": 0,
        "tacklesElo": initialEloList,
        "tflElo": initialEloList,
        "sackElo": initialEloList,
        "intElo": initialEloList,
        "pbuElo": initialEloList,
        "ffElo": initialEloList,
        "date": initialDateList,
        "opp": initialOppList,
    }


# Default dictionary for teams
teams_default = {
    "last": initialTeamElo,
    "elo": [initialTeamElo],
    "date": initialDateList,
    "opp": initialOppList,
}

# Demographic variables to merge onto players
demos = []#["position"]

extension = ".csv"

playerstatroot = "ncaa-player-game-statistics"
teamstatroot = "ncaa-team-game-statistics"

# Do all the work - looping over each season
from tqdm import tqdm
tqdm.write("Beginning ComputeElo")
seasons = tqdm(seasons)
for season in seasons:
    seasons.set_description(f"s: {season}")

    # Read in files for the current season
    playerstatfile = Path("data_raw", "cfb", f"{playerstatroot}{season}{extension}")
    teamstatfile = Path("data_raw", "cfb", f"{teamstatroot}{season}{extension}")

    teamstats = eu.readteamgamedata(teamstatfile)

    # Read in and merge player stats with demographics/position
    playerstats = eu.readplayergamestats(playerstatfile)

    dates = [int(x) for x in sorted(list(playerstats["gamedate"].unique()))]

    # Filter based on stats
    # FUTURE: change here to process based on player's position.
    # FUTURE: [player["position"].rsplit("/")] -- returns a list of positions the player has.
    if processRBs:
        # RBs - Filter to only players with at least 1 Rush Attempt
        rbstats = playerstats[playerstats["Rush Att"] > 0].copy()

    if processWRs:
        # WRs/TEs - Filter to only players with at least 1 Catch
        wrstats = playerstats[playerstats["Rec"] > 0].copy()

    if processQBs:
        # QBs - Filter to only players with at least 1 Pass Attempt
        qbstats = playerstats[playerstats["Pass Att"] > 0].copy()

    if processDefense:
        # Defense - Filter to only players with some kind of tackle
        defstats = playerstats[
            (playerstats["Tackle Solo"] > 0) | (playerstats["Tackle Assist"] > 0)
        ].copy()

    #### Date in Dates
    # Loop over each date within the season - want to update Elo after each game
    for date in dates:
        rb_teams = set()
        wr_teams = set()
        qb_teams = set()
        def_teams = set()
        # Grab the players/games only on this date
        if processRBs:
            rbs = rbstats[rbstats["gamedate"] == date]
            rb_teams = set(rbs["awayteam"]).union(set(rbs["hometeam"]))
            rbs = rbs.to_dict("records")
        if processWRs:
            wrs = wrstats[wrstats["gamedate"] == date]
            wr_teams = set(wrs["awayteam"]).union(set(wrs["hometeam"]))
            wrs = wrs.to_dict("records")
        if processQBs:
            qbs = qbstats[qbstats["gamedate"] == date]
            qb_teams = set(qbs["awayteam"]).union(set(qbs["hometeam"]))
            qbs = qbs.to_dict("records")
        if processDefense:
            defs = defstats[defstats["gamedate"] == date]
            def_teams = set(defs["awayteam"]).union(set(defs["hometeam"]))
            defs = defs.to_dict("records")

        # Create needed game dictionaries for teams
        position_teams = rb_teams.union(wr_teams).union(qb_teams).union(def_teams)

        teams = teamstats[teamstats["gamedate"] == date].copy()
        teams_to_check = (
            set(teams["awayteam"]).union(set(teams["hometeam"])).union(position_teams)
        )

        # Initialize needed dictionaries
        for k in teams_to_check:
            adjust = False
            if BOOST_POWER_5 and k in power5teams:
                adjust = True
            if k not in rushD:
                rushD[k] = deepcopy(teams_default)
                rushD[k]["Team Code"] = k
                if adjust:
                    rushD[k]["elo"][0] = power5InitialTeamElo
            if k not in rushO:
                rushO[k] = deepcopy(teams_default)
                rushO[k]["Team Code"] = k
                if adjust:
                    rushO[k]["elo"][0] = power5InitialTeamElo
            if k not in passD:
                passD[k] = deepcopy(teams_default)
                passD[k]["Team Code"] = k
                if adjust:
                    passD[k]["elo"][0] = power5InitialTeamElo
            if k not in passO:
                passO[k] = deepcopy(teams_default)
                passO[k]["Team Code"] = k
                if adjust:
                    passO[k]["elo"][0] = power5InitialTeamElo

        # First do player evaluations - Must do this before we update the team values
        teams = teams.to_dict("records")

        #### RB Evaluation
        if processRBs and len(rbs) > 0:
            for rb in rbs:
                # Player's `unique_id` and opponent team's `Team Code`
                me_id = rb["unique_id"]
                them_id = rb["Team Code opp"]

                # Initialize a player dictionary if not already present
                if me_id not in rushers:
                    rushers[me_id] = deepcopy(rushers_default)
                    rushers[me_id]["unique_id"] = me_id
                    # Add the demographic/position variables
                    for demo in demos:
                        rushers[me_id][demo] = rb[demo]

                # Save the most current Rush D Elo
                rushDelo = rushD[them_id]["elo"][-1]

                #### RBs: Rush Yards Per Game Elo
                # Calculate point differential - in this case yards
                ptdiff = abs(rb["Rush Yard"] - rushPdrawline) * yardFactor
                # Win if exceeded the drawline
                win = rb["Rush Yard"] > rushPdrawline
                # Grab current YPG Elo
                ypgElo = rushers[me_id]["ypgElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(ypgElo, rushDelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                rushers[me_id]["ypgElo"] = rushers[me_id]["ypgElo"] + [Elo]
                rushers[me_id]["lastYPG"] = Elo
                # Add to the number of wins and losses
                if win:
                    rushers[me_id]["wYPG"] += 1
                else:
                    rushers[me_id]["lYPG"] += 1

                #### RBs: Yards Per Carry Elo
                # Adjusted yards per carry based on log10 of number of carries
                ypc = log10(rb["Rush Att"]) * (rb["Rush Yard"] / rb["Rush Att"])
                # Compute point difference of win
                ptdiff = abs(ypc - ydPerCarry) * ypcFactor
                # Is the ypc a win?
                win = ypc > ydPerCarry
                # Grab current YPC Elo
                ypcElo = rushers[me_id]["ypcElo"][-1]
                # Compute new YPC Elo
                Elo = eu.updateElo(ypcElo, rushDelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                rushers[me_id]["ypcElo"] = rushers[me_id]["ypcElo"] + [Elo]
                rushers[me_id]["lastYPC"] = Elo
                if win:
                    rushers[me_id]["wYPC"] += 1
                else:
                    rushers[me_id]["lYPC"] += 1

                #### RBs: Yards From Scrimmage Elo
                # Compute yards from scrimmage
                yfs = rb["Rush Yard"] + rb["Rec Yards"]
                # Compute point diff
                ptdiff = abs(yfs - yfsPdrawline) * yardFactor
                # Win if exceeded drawline
                win = yfs > yfsPdrawline
                # Grab current YFS Elo
                yfsElo = rushers[me_id]["yfsElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(yfsElo, rushDelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                rushers[me_id]["yfsElo"] = rushers[me_id]["yfsElo"] + [Elo]
                rushers[me_id]["lastYFS"] = Elo
                if win:
                    rushers[me_id]["wYFS"] += 1
                else:
                    rushers[me_id]["lYFS"] += 1

                #### RBs: TD Elo
                # Sum all TDs
                td = (
                    rb["Rush TD"] + rb["Rec TD"]
                )  # + rb["Kickoff Ret TD"] + rb["Punt Ret TD"]
                # Compute point diff
                ptdiff = abs(td - TDline) * 10
                # Win if exceeds drawline
                win = td > TDline
                # Grab current TD Elo
                tdElo = rushers[me_id]["tdElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(tdElo, rushDelo, ptdiff, win, blowoutFactor, playerK)

                # Update Values
                rushers[me_id]["tdElo"] = rushers[me_id]["tdElo"] + [Elo]
                rushers[me_id]["lastTD"] = Elo
                if win:
                    rushers[me_id]["wTD"] += 1
                else:
                    rushers[me_id]["lTD"] += 1

                #### RBs: Dates & Cumulative Elo
                # Accumulate Elo scores into a cumulative Elo
                ypgElo = rushers[me_id]["ypgElo"][-1]
                ypcElo = rushers[me_id]["ypcElo"][-1]
                yfsElo = rushers[me_id]["yfsElo"][-1]
                tdElo = rushers[me_id]["tdElo"][-1]
                # Compute Composite Elo
                value = round(((ypgElo + yfsElo) / 2 + ypcElo + tdElo) / 3)
                # Update values
                rushers[me_id]["eloC"] = rushers[me_id]["eloC"] + [value]
                rushers[me_id]["last"] = value
                # Add the date (if first time, add a date for the first date)
                if rushers[me_id]["count"] == 0:
                    rushers[me_id]["date"][0] = eu.get_start_date(date)
                rushers[me_id]["date"] = rushers[me_id]["date"] + [date]

                # Add the opponent
                rushers[me_id]["opp"] = rushers[me_id]["opp"] + [rb["Team Code opp"]]

                # Count # of games the player has data for
                rushers[me_id]["count"] += 1

        #### WR/TE Evaluation
        if processWRs and len(wrs) > 0:
            for wr in wrs:
                # Player's `unique_id` and opponent team's `Team Code`
                me_id = wr["unique_id"]
                them_id = wr["Team Code opp"]

                # Initialize a player dictionary if not already present
                if me_id not in receivers:
                    receivers[me_id] = deepcopy(receivers_default)
                    receivers[me_id]["unique_id"] = me_id
                    # Add the demographic/position variables
                    for demo in demos:
                        receivers[me_id][demo] = wr[demo]

                # Save the most current Pass D Elo
                passDelo = passD[them_id]["elo"][-1]

                #### WR/TE: Receptions Per Game Elo
                # Calculate point differential - in this case yards
                ptdiff = abs(wr["Rec"] - recDrawline) * 5
                # Win if exceeded the drawline
                win = wr["Rec"] > recDrawline
                # Grab current YPG Elo
                recElo = receivers[me_id]["recElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(recElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                receivers[me_id]["recElo"] = receivers[me_id]["recElo"] + [Elo]
                receivers[me_id]["lastRec"] = Elo
                if win:
                    receivers[me_id]["wRec"] += 1
                else:
                    receivers[me_id]["lRec"] += 1

                #### WR/TE: Receiving Yards Per Game Elo
                # Calculate point differential - in this case yards
                ptdiff = abs(wr["Rec Yards"] - recPdrawline) * yardFactor
                # Win if exceeded the drawline
                win = wr["Rec Yards"] > recPdrawline
                # Grab current YPG Elo
                ypgElo = receivers[me_id]["ypgElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(ypgElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                receivers[me_id]["ypgElo"] = receivers[me_id]["ypgElo"] + [Elo]
                receivers[me_id]["lastYPG"] = Elo
                if win:
                    receivers[me_id]["wYPG"] += 1
                else:
                    receivers[me_id]["lYPG"] += 1

                #### WR/TE:  Yards Per Catch Elo
                # Compute adjusted yards per catch based on log10 of 2 times number of catches
                ypc = wr["Rec Yards"] / wr["Rec"]
                # Compute the point difference of win
                ptdiff = abs(ypc - ydPerCatchDrawline) * ypcFactor
                # Is the ypc a win?
                win = ypc > ydPerCatchDrawline
                # Grab current YPC Elo
                ypcElo = receivers[me_id]["ypcElo"][-1]
                # Compute new YPC Elo
                Elo = eu.updateElo(ypcElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                receivers[me_id]["ypcElo"] = receivers[me_id]["ypcElo"] + [Elo]
                receivers[me_id]["lastYPC"] = Elo
                if win:
                    receivers[me_id]["wYPC"] += 1
                else:
                    receivers[me_id]["lYPC"] += 1

                #### WR/TE: TD Elo
                # Sum all TDs
                td = wr["Rec TD"]  # + wr$Kickoff.Ret.TD + wr$Punt.Ret.TD
                # Compute point diff
                ptdiff = abs(td - TDline) * 10
                # Win if exceeds drawline
                win = td > TDline
                # Grab current TD Elo
                tdElo = receivers[me_id]["tdElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(tdElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                # Update Values
                receivers[me_id]["tdElo"] = receivers[me_id]["tdElo"] + [Elo]
                receivers[me_id]["lastTD"] = Elo
                if win:
                    receivers[me_id]["wTD"] += 1
                else:
                    receivers[me_id]["lTD"] += 1

                #### WR/TE: Dates & Cumulative Elo
                # Accumulate ELO scores into a cumulative ELO
                recElo = receivers[me_id]["recElo"][-1]
                ypgElo = receivers[me_id]["ypgElo"][-1]
                ypcElo = receivers[me_id]["ypcElo"][-1]
                tdElo = receivers[me_id]["tdElo"][-1]
                # Compute Composite Elo
                value = round((recElo + ypgElo + ypcElo + tdElo) / 4)
                # Update values
                receivers[me_id]["eloC"] = receivers[me_id]["eloC"] + [value]
                receivers[me_id]["last"] = value

                # Add the date (if first time, add a date for the first date)
                if receivers[me_id]["count"] == 0:
                    receivers[me_id]["date"][0] = eu.get_start_date(date)
                receivers[me_id]["date"] = receivers[me_id]["date"] + [date]

                # Add the opponent
                receivers[me_id]["opp"] = receivers[me_id]["opp"] + [
                    wr["Team Code opp"]
                ]
                # Count # of games the player has data for
                receivers[me_id]["count"] += 1

        #### QB Evaluation
        if processQBs and len(qbs) > 0:
            for qb in qbs:
                # Player's `unique_id` and opponent team's `Team Code`
                me_id = qb["unique_id"]
                them_id = qb["Team Code opp"]

                # Initialize a player dictionary if not already present
                if me_id not in passers:
                    passers[me_id] = deepcopy(passers_default)
                    passers[me_id]["unique_id"] = me_id
                    # Add the demographic/position variables
                    for demo in demos:
                        passers[me_id][demo] = qb[demo]

                # Save the most current Pass D ELO
                passDelo = passD[them_id]["elo"][-1]

                #### QBs: QBR Elo
                # Compute QBR - From https://en.wikipedia.org/wiki/Passer_rating using NCAA rating formula
                qbr = eu.calcQBR(
                    qb["Pass Yard"],
                    qb["Pass TD"],
                    qb["Pass Comp"],
                    qb["Pass Int"],
                    qb["Pass Att"],
                )
                # Compute point diff
                ptdiff = abs(qbr - qbrDrawline)
                # Win if exceeds drawline
                win = qbr > qbrDrawline
                # Grab current QBR Elo
                qbrElo = passers[me_id]["qbrElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(qbrElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                # Update Values
                passers[me_id]["qbrElo"] = passers[me_id]["qbrElo"] + [Elo]
                passers[me_id]["lastQBR"] = Elo
                if win:
                    passers[me_id]["wQBR"] += 1
                else:
                    passers[me_id]["lQBR"] += 1

                #### QBs: Pass Yards Per Game Elo
                # Pass Yards
                passYards = qb["Pass Yard"]
                # Compute point diff
                ptdiff = abs(passYards - passDdrawline) / passYardFactor
                # Win if exceeds drawline
                win = passYards > passDdrawline
                # Grab current YPG Elo
                ypgElo = passers[me_id]["ypgElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(ypgElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                # Update Values
                passers[me_id]["ypgElo"] = passers[me_id]["ypgElo"] + [Elo]
                passers[me_id]["lastYPG"] = Elo
                if win:
                    passers[me_id]["wYPG"] += 1
                else:
                    passers[me_id]["lYPG"] += 1

                #### QBs: Yards Per Catch Elo
                # Compute Yards Per Catch
                if qb["Pass Comp"] > 0:
                    ypc = qb["Pass Yard"] / qb["Pass Comp"]
                else:
                    ypc = 0
                # Compute point diff
                ptdiff = abs(ypc - ydPerCatchDrawline) * passYPCFactor
                # Win if exceeds drawline
                win = ypc > ydPerCatchDrawline
                # Grab current YPC Elo
                ypcElo = passers[me_id]["ypcElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(ypcElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                passers[me_id]["ypcElo"] = passers[me_id]["ypcElo"] + [Elo]
                passers[me_id]["lastYPC"] = Elo
                if win:
                    passers[me_id]["wYPC"] += 1
                else:
                    passers[me_id]["lYPC"] += 1

                #### QBs: Yards Per Attempt Elo
                # Compute Yards Per Attempt
                ypa = qb["Pass Yard"] / qb["Pass Att"]
                # Compute point diff
                ptdiff = abs(ypa - ydPerAttemptDrawline) * passYPCFactor
                # Win if exceeds drawline
                win = ypa > ydPerAttemptDrawline
                # Grab current YPA Elo
                ypaElo = passers[me_id]["ypaElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(ypaElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                passers[me_id]["ypaElo"] = passers[me_id]["ypaElo"] + [Elo]
                passers[me_id]["lastYPA"] = Elo
                if win:
                    passers[me_id]["wYPA"] += 1
                else:
                    passers[me_id]["lYPA"] += 1

                #### QBs: Completion Percentage Elo
                # Compute Completion Percentage
                pct = qb["Pass Comp"] / qb["Pass Att"] * 100.0
                # Compute point diff
                ptdiff = abs(pct - compPercDrawline) * pctFactor
                # Win if exceeds drawline
                win = pct > compPercDrawline
                # Grab current YPC Elo
                pctElo = passers[me_id]["pctElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(pctElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                passers[me_id]["pctElo"] = passers[me_id]["pctElo"] + [Elo]
                passers[me_id]["lastPct"] = Elo
                if win:
                    passers[me_id]["wPCT"] += 1
                else:
                    passers[me_id]["lPCT"] += 1

                #### QBs: Pass TD Elo
                # Compute Pass TD
                td = qb["Pass TD"] + qb["Rush TD"]
                # Compute point diff
                ptdiff = abs(td - passTDline) * tdFactor
                # Win if exceeds drawline
                win = td > passTDline
                # Grab current TD Elo
                tdElo = passers[me_id]["tdElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(tdElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                passers[me_id]["tdElo"] = passers[me_id]["tdElo"] + [Elo]
                passers[me_id]["lastTD"] = Elo
                if win:
                    passers[me_id]["wTD"] += 1
                else:
                    passers[me_id]["lTD"] += 1

                #### QBs: Pass Int Elo
                # Compute Pass Int
                intr = qb["Pass Int"]
                # Compute point diff
                ptdiff = abs(intr - passIntline) * intFactor
                # Win if exceeds drawline
                win = intr < passIntline
                # Grab current Int Elo
                intElo = passers[me_id]["intElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(intElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                passers[me_id]["intElo"] = passers[me_id]["intElo"] + [Elo]
                passers[me_id]["lastInt"] = Elo
                if win:
                    passers[me_id]["wINT"] += 1
                else:
                    passers[me_id]["lINT"] += 1

                #### QB: Dates & Cumulative Elo
                # Accumulate Elo scores into a cumulative Elo
                pctElo = passers[me_id]["pctElo"][-1]
                ypgElo = passers[me_id]["ypgElo"][-1]
                ypaElo = passers[me_id]["ypaElo"][-1]
                tdElo = passers[me_id]["tdElo"][-1]
                intElo = passers[me_id]["intElo"][-1]
                # Compute Composite Elo
                value = round((pctElo + ypgElo + ypaElo + tdElo + intElo) / 5)
                # Update values
                passers[me_id]["eloC"] = passers[me_id]["eloC"] + [value]
                passers[me_id]["last"] = value

                # Add the date (if first time, add a date for the first date)
                if passers[me_id]["count"] == 0:
                    passers[me_id]["date"][0] = eu.get_start_date(date)
                passers[me_id]["date"] = passers[me_id]["date"] + [date]

                # Add the opponent
                passers[me_id]["opp"] = passers[me_id]["opp"] + [qb["Team Code opp"]]

                # Count # of games the player has data for
                passers[me_id]["count"] += 1

        #### Defense Evaluation
        if processDefense and len(defs) > 0:
            for defn in defs:
                # Player's `unique_id` and opponent team's `Team Code`
                me_id = defn["unique_id"]
                them_id = defn["Team Code opp"]

                # Initialize a player dictionary if not already present
                if me_id not in defense:
                    defense[me_id] = deepcopy(defense_default)
                    defense[me_id]["unique_id"] = me_id
                    # Add the demographic/position variables
                    for demo in demos:
                        defense[me_id][demo] = defn[demo]

                # Save the most current Rush O ELO and Pass O ELO
                rushOelo = rushO[them_id]["elo"][-1]
                passOelo = passO[them_id]["elo"][-1]

                #### DEF: Tackles Elo
                # Calculate total tackles
                tackles = defn["Tackle Solo"] + (0.5 * defn["Tackle Assist"])
                # Calculate point differential - in this case tackles
                ptdiff = abs(tackles - tackleDrawline) * 4
                # Win if exceeds drawline
                win = tackles > tackleDrawline
                # Grab current tackle Elo
                tacklesElo = defense[me_id]["tacklesElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(tacklesElo, rushOelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                defense[me_id]["tacklesElo"] = defense[me_id]["tacklesElo"] + [Elo]
                defense[me_id]["lastTackles"] = Elo
                if win:
                    defense[me_id]["wTackles"] += 1
                else:
                    defense[me_id]["lTackles"] += 1

                #### DEF: Tackle For Loss Elo
                # Calculate TFL
                tfl = defn["Tackle for Loss"]
                # Calculate point differential - in this case tackles
                ptdiff = abs(tfl - tflDrawline) * 10
                # Win if exceeds drawline
                win = tfl > tflDrawline
                # Grab current TFL Elo
                tflElo = defense[me_id]["tflElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(tflElo, rushOelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                defense[me_id]["tflElo"] = defense[me_id]["tflElo"] + [Elo]
                defense[me_id]["lastTFL"] = Elo
                if win:
                    defense[me_id]["wTFL"] += 1
                else:
                    defense[me_id]["lTFL"] += 1

                #### DEF: Sack Elo
                # Calculate Sacks
                sack = defn["Sack"]
                # Calculate point differential - in this case tackles
                ptdiff = abs(sack - sackDrawline) * 10
                # Win if exceeds drawline
                win = sack >= sackDrawline
                # Grab current TFL Elo
                sackElo = defense[me_id]["sackElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(sackElo, passOelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                defense[me_id]["sackElo"] = defense[me_id]["sackElo"] + [Elo]
                defense[me_id]["lastSack"] = Elo
                if win:
                    defense[me_id]["wSack"] += 1
                else:
                    defense[me_id]["lSack"] += 1

                #### DEF: Int Elo
                # Calculate Ints
                intr = defn["Int Ret"]
                # Calculate point differential - in this case interceptions
                ptdiff = abs(intr - intDrawline) * 10
                # Win if exceeds drawline
                win = intr >= intDrawline
                # Grab current TFL Elo
                intElo = defense[me_id]["intElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(intElo, passOelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                defense[me_id]["intElo"] = defense[me_id]["intElo"] + [Elo]
                defense[me_id]["lastInt"] = Elo
                if win:
                    defense[me_id]["wINT"] += 1
                else:
                    defense[me_id]["lINT"] += 1

                #### DEF: Pass Broken Up Elo
                # Calculate Passes Broken Up
                pbu = defn["Pass Broken Up"]
                # Calculate point differential - in this case interceptions
                ptdiff = abs(pbu - pbuDrawline) * 5
                # Win if exceeds drawline
                win = pbu > pbuDrawline
                # Grab current PBU Elo
                pbuElo = defense[me_id]["pbuElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(pbuElo, passOelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                defense[me_id]["pbuElo"] = defense[me_id]["pbuElo"] + [Elo]
                defense[me_id]["lastPBU"] = Elo
                if win:
                    defense[me_id]["wPBU"] += 1
                else:
                    defense[me_id]["lPBU"] += 1

                #### DEF: Forced Fumble Elo
                # Calculate Passes Broken Up
                ff = defn["Fumble Forced"]
                # Calculate point differential - in this case interceptions
                ptdiff = abs(ff - ffDrawline) * 10
                # Win if exceeds drawline
                win = ff >= ffDrawline
                # Grab current PBU Elo
                ffElo = defense[me_id]["ffElo"][-1]
                # Compute new Elo
                Elo = eu.updateElo(ffElo, rushOelo, ptdiff, win, blowoutFactor, playerK)

                # Update values
                defense[me_id]["ffElo"] = defense[me_id]["ffElo"] + [Elo]
                defense[me_id]["lastFF"] = Elo
                if win:
                    defense[me_id]["wFF"] += 1
                else:
                    defense[me_id]["lFF"] += 1

                #### DEF: Dates
                # Add the date (if first time, add a date for the first date)
                if defense[me_id]["count"] == 0:
                    defense[me_id]["date"][0] = eu.get_start_date(date)
                defense[me_id]["date"] = defense[me_id]["date"] + [date]

                # Add the opponent
                defense[me_id]["opp"] = defense[me_id]["opp"] + [defn["Team Code opp"]]

                # Count # of games the player has data for
                defense[me_id]["count"] += 1

        #### Team rush D and rush O (overall) Evaluation
        if len(teams) > 0:
            for team in teams:
                # Codes for the defensive and offensive teams
                us_id = team["Team Code"]
                them_id = team["Team Code opp"]

                # Grab current Elo values
                rushDelo = rushD[us_id]["elo"][-1]
                rushOelo = rushO[them_id]["elo"][-1]
                passDelo = passD[us_id]["elo"][-1]
                passOelo = passO[them_id]["elo"][-1]

                #### RushD/RushO
                # Team performance based on yards per game
                # Calculate point differential - in this case yards
                ptdiff = abs(team["Rush Yard opp"] - rushDdrawline) / yardFactor

                # Win if held offense to less than drawline
                win = team["Rush Yard opp"] < rushDdrawline

                # Compute new Elos
                dElo, oElo = eu.updateElo(
                    rushDelo, rushOelo, ptdiff, win, blowoutFactor, teamK, both=True
                )

                # Add the date (if first time, add a date for the first date)
                if len(rushD[us_id]["date"]) == 1:
                    rushD[us_id]["date"][0] = eu.get_start_date(date)
                if len(rushO[them_id]["date"]) == 1:
                    rushO[them_id]["date"][0] = eu.get_start_date(date)
                # Update date
                rushD[us_id]["date"] = rushD[us_id]["date"] + [date]
                rushO[them_id]["date"] = rushO[them_id]["date"] + [date]

                # Update opponent
                rushD[us_id]["opp"] = rushD[us_id]["opp"] + [them_id]
                rushO[them_id]["opp"] = rushO[them_id]["opp"] + [us_id]

                # Update values
                rushD[us_id]["elo"] = rushD[us_id]["elo"] + [dElo]
                rushO[them_id]["elo"] = rushO[them_id]["elo"] + [oElo]

                rushD[us_id]["last"] = dElo
                rushO[them_id]["last"] = oElo

                #### PassD/PassO
                # Team performance based on yards per game
                # Calculate point differential - in this case yards
                ptdiff = abs(team["Pass Yard opp"] - passDdrawline) / passYardFactor

                # Win if held offense to less than drawline
                win = team["Pass Yard opp"] < passDdrawline

                # Compute new Elos
                dElo, oElo = eu.updateElo(
                    passDelo, passOelo, ptdiff, win, blowoutFactor, teamK, both=True
                )

                # Add the date (if first time, add a date for the first date)
                if len(passD[us_id]["date"]) == 1:
                    passD[us_id]["date"][0] = eu.get_start_date(date)
                if len(passO[them_id]["date"]) == 1:
                    passO[them_id]["date"][0] = eu.get_start_date(date)
                # Update date
                passD[us_id]["date"] = passD[us_id]["date"] + [date]
                passO[them_id]["date"] = passO[them_id]["date"] + [date]
                
                # Update opponent
                passD[us_id]["opp"] = passD[us_id]["opp"] + [them_id]
                passO[them_id]["opp"] = passO[them_id]["opp"] + [us_id]

                # Update values
                passD[us_id]["elo"] = passD[us_id]["elo"] + [dElo]
                passO[them_id]["elo"] = passO[them_id]["elo"] + [oElo]

                passD[us_id]["last"] = dElo
                passO[them_id]["last"] = oElo

# Make into dataframes
rushD = pd.DataFrame.from_dict(rushD, orient="index").reset_index(drop=True)
rushO = pd.DataFrame.from_dict(rushO, orient="index").reset_index(drop=True)
passD = pd.DataFrame.from_dict(passD, orient="index").reset_index(drop=True)
passO = pd.DataFrame.from_dict(passO, orient="index").reset_index(drop=True)

if processRBs:
    rushers = pd.DataFrame.from_dict(rushers, orient="index").reset_index(drop=True)
if processWRs:
    receivers = pd.DataFrame.from_dict(receivers, orient="index").reset_index(drop=True)
if processQBs:
    passers = pd.DataFrame.from_dict(passers, orient="index").reset_index(drop=True)
if processDefense:
    defense = pd.DataFrame.from_dict(defense, orient="index").reset_index(drop=True)
