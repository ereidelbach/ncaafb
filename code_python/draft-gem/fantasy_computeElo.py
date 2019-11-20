#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Capture operations for processing ELO
import json
import pandas as pd
import numpy as np
from copy import deepcopy
from math import floor, log, log10, sqrt
from pathlib import Path

import eloUtilities as eu
from fantasy_runOldSeasons import rushD, rushO, passD, passO

with open(Path("elo_config.json")) as file:
    cfg = json.load(file)["computeElo"]["fantasy"]

# Groups to process
processRBs = True
processWRs = True
processTEs = True
processQBs = True
processDefense = False

blowoutFactor = True
teamBlowoutFactor = True

# Regression variables
regress = True
# Whether to add additional data point for regression
addRegressionDataPoint = False

if True: # rolls up config code--can remove if restructuring
    # Configuration
    initialPlayerElo = 1300
    initialTeamElo = 1300
    playerK = 20
    teamK = 20

    season = 0
    thisSeason = 0

    # Fantasy Team Drawlines
    rushDdrawline = 106
    rushDTDline = 1
    passDdrawline = 231
    passDTDline = 1

    # Fantasy RB Drawlines
    rushPdrawline = 63      # median of NFL RB/HB/FB
    rushRecDrawline = 2     # median of NFL RB/HB/FB
    rushRecYdDrawline = 15  # median of NFL RB/HB/FB
    yfsPdrawline = 86       # median of NFL > 9 carries
    # NOTE: was commented out before and causing issues
    rushTDdrawline = 0.574

    # Fantasy WR Drawlines
    recPdrawline = 37       # median of NFL > 2 rec
    recDrawline = 3
    recTDdrawline = 0.288

    # Fantasy TE Drawlines
    teYpgdrawline = 24
    teRecDrawline = 2
    teTDdrawline = 0.25

    # Fantasy QB Drawlines
    passDrawline = 231
    qbRushDrawline = 5
    passTDline = 1
    qbRushTDline = 0.1
    passIntDrawline = 1

    TDline = 0.5

    yardFactor = 1
    ypcFactor = 5
    fumline = 0.1

    tackleDrawline = 2.0 # Won't give credit for 2 tackles
    tflDrawline = 0.3
    sackDrawline = 0
    intDrawline = 0
    ffDrawline = 0
    pbuDrawline = 0.1
    tdFactor = 5
    intFactor = 5

    # Fantasy stats
    rbYPGwins = 0
    rbYPGlosses = 0
    rbYFSwins = 0
    rbYFSlosses = 0
    rbTDwins = 0
    rbTDlosses = 0
    rbRatio = []
    rbRecRatio = []
    rbRecYdsRatio = []
    rbTDRatio = []

    wrRatio = []
    qbYPGwins = 0
    qbYPGlosses = 0
    qbRatio = []
    rbSqError = 0
    rbCount = 0
    rbSqErrorYFS = 0
    rbRecSqError = 0
    rbCountYFS = 0
    rbRecCount = 0
    wrSqError = 0
    wrCount = 0
    qbSqError = 0
    qbCount = 0
    rbError = []
    rbTDSqError = 0
    rbTDCount = 0
    rbTDError = []
    rbTDPred = []
    rbTDAct = []
    rbDKSqError = 0
    rbDKCount = 0
    rbDKError = []
    rbDKPred = []
    rbDKAct = []
    rbTDEloDiff = []
    rbErrorYFS = []
    rbRecError = []
    wrError = []
    qbError = []
    rbPred = []
    rbPredYFS = []
    rbRecPred = []
    wrPred = []
    qbPred = []
    rbAct = []
    rbActYFS = []
    rbRecAct = []
    wrAct = []
    qbAct = []
    rbEloDiff = []
    yfsEloDiff = []
    rbRecEloDiff = []
    wrEloDiff = []
    qbEloDiff = []

    # WR Arrays for fantasy tracking error
    wrRecError = []
    wrRecRatio = []
    wrRecPred= []
    wrRecAct= []
    wrRecEloDiff = []
    wrRecCount = 0
    wrRecSqError = 0
    wrYpgError = []
    wrYpgRatio = []
    wrYpgPred= []
    wrYpgAct= []
    wrYpgEloDiff = []
    wrYpgCount = 0
    wrYpgSqError = 0
    wrTDError = []
    wrTDRatio = []
    wrTDPred= []
    wrTDAct= []
    wrTDEloDiff = []
    wrTDCount = 0
    wrTDSqError = 0

    wrYPGwins = 0
    wrYPGlosses = 0
    wrTDwins = 0
    wrTDlosses = 0

    wrDKSqError = 0
    wrDKCount = 0
    wrDKError = []
    wrDKPred = []
    wrDKAct = []

    # TE Arrays for fantasy tracking error
    teRecError = []
    teRecRatio = []
    teRecPred= []
    teRecAct= []
    teRecEloDiff = []
    teRecCount = 0
    teRecSqError = 0
    teYpgError = []
    teYpgRatio = []
    teYpgPred= []
    teYpgAct= []
    teYpgEloDiff = []
    teYpgCount = 0
    teYpgSqError = 0
    teTDError = []
    teTDRatio = []
    teTDPred= []
    teTDAct= []
    teTDEloDiff = []
    teTDCount = 0
    teTDSqError = 0

    teYPGwins = 0
    teYPGlosses = 0
    teTDwins = 0
    teTDlosses = 0

    teDKSqError = 0
    teDKCount = 0
    teDKError = []
    teDKPred = []
    teDKAct = []

    # QB Fantasy Stuff
    qbYpgError = []
    qbYpgRatio = []
    qbYpgPred= []
    qbYpgAct= []
    qbYpgEloDiff = []
    qbYpgCount = 0
    qbYpgSqError = 0
    qbRushYpgError = []
    qbRushYpgRatio = []
    qbRushYpgPred= []
    qbRushYpgAct= []
    qbRushYpgEloDiff = []
    qbRushYpgCount = 0
    qbRushYpgSqError = 0
    qbTDError = []
    qbTDRatio = []
    qbTDPred= []
    qbTDAct= []
    qbTDEloDiff = []
    qbTDCount = 0
    qbTDSqError = 0
    qbRushTDError = []
    qbRushTDRatio = []
    qbRushTDPred= []
    qbRushTDAct= []
    qbRushTDEloDiff = []
    qbRushTDCount = 0
    qbRushTDSqError = 0
    qbIntError = []
    qbIntRatio = []
    qbIntPred= []
    qbIntAct= []
    qbIntEloDiff = []
    qbIntCount = 0
    qbIntSqError = 0

    qbYPGwins = 0
    qbYPGlosses = 0
    qbRushYPGwins = 0
    qbRushYPGlosses = 0
    qbTDwins = 0
    qbTDlosses = 0
    qbRushTDwins = 0
    qbRushTDlosses = 0
    qbINTwins = 0
    qbINTlosses = 0

    qbDKSqError = 0
    qbDKCount = 0
    qbDKError = []
    qbDKPred = []
    qbDKAct = []

# Starter values
initialEloList = [initialPlayerElo]
initialDateList = [0]
initialOppList = [0]

# Holding and default dictionaries for each type of players
if processRBs:
    fRBs = {}
    # Initialize Rushing Elo data
    fRBs_default = {
        "wYPG": 0,
        "lYPG": 0,
        "wYFS": 0,
        "lYFS": 0,
        "wTD": 0,
        "lTD": 0,
        "lastYPG": initialPlayerElo,
        "lastYFS": initialPlayerElo,
        "lastTD": initialPlayerElo,
        "count": 0,
        "ypgElo": initialEloList,
        "recElo": initialEloList,
        "recYpgElo": initialEloList,
        "tdElo": initialEloList,
        "date": initialDateList,
        "season": [0],
        "game": [0],
        "week": [0],
        "rushYdsPred" : [0],
        "rushYdsAct" : [0],
        "recYdsPred" : [0],
        "recYdsAct" : [0],
        "recPred" : [0],
        "recAct" : [0],
        "tdPred" : [0],
        "tdAct" : [0],
        "dk" : [0],
        "dkAct" : [0],
        "ppr" : [0],
        "pprAct" : [0],
        "fd": [0],
        "fdAct": [0],
        "std": [0],
        "stdAct": [0],
        "opp": initialOppList
    }

if processWRs:
    fWRs = {}
    # Initialize Receiving Elo data
    fWRs_default = {
        "wRec": 0,
        "lRec": 0,
        "wYPG": 0,
        "lYPG": 0,
        "wTD": 0,
        "lTD": 0,
        "lastRec": initialPlayerElo,
        "lastYPG": initialPlayerElo,
        "lastTD": initialPlayerElo,
        "count": 0,
        "recElo": initialEloList,
        "ypgElo": initialEloList,
        "tdElo": initialEloList,
        "date": initialDateList,
        "season": [0],
        "game": [0],
        "week": [0],
        "ypgPred": [0],
        "ypgAct": [0],
        "recPred": [0],
        "recAct": [0],
        "tdPred": [0],
        "tdAct": [0],
        "dk": [0],
        "dkAct": [0],
        "ppr" : [0],
        "pprAct" : [0],
        "fd": [0],
        "fdAct": [0],
        "std": [0],
        "stdAct": [0],
        "opp": initialOppList
    }

if processTEs:
    fTEs = {}
    # Initialize Receiving Elo data
    fTEs_default = {
        "wRec": 0,
        "lRec": 0,
        "wYPG": 0,
        "lYPG": 0,
        "wTD": 0,
        "lTD": 0,
        "lastRec": initialPlayerElo,
        "lastYPG": initialPlayerElo,
        "lastTD": initialPlayerElo,
        "count": 0,
        "recElo": initialEloList,
        "ypgElo": initialEloList,
        "tdElo": initialEloList,
        "date": initialDateList,
        "season": [0],
        "game": [0],
        "week": [0],
        "ypgPred": [0],
        "ypgAct": [0],
        "recPred": [0],
        "recAct": [0],
        "tdPred": [0],
        "tdAct": [0],
        "dk": [0],
        "dkAct": [0],
        "ppr" : [0],
        "pprAct" : [0],
        "fd": [0],
        "fdAct": [0],
        "std": [0],
        "stdAct": [0],
        "opp": initialOppList
    }

if processQBs:
    fQBs = {}
    # Initialize Passing Elo data
    fQBs_default = {
        "wYPG": 0,
        "lYPG": 0,
        "wRushYPG": 0,
        "lRushYPG": 0,
        "wTD": 0,
        "lTD": 0,
        "wRushTD": 0,
        "lRushTD": 0,
        "wINT": 0,
        "lINT": 0,
        "lastYPG": initialPlayerElo,
        "lastRushYPG": initialPlayerElo,
        "lastTD": initialPlayerElo,
        "lastRushTD": initialPlayerElo,
        "lastInt": initialPlayerElo,
        "count": 0,
        "ypgElo": initialEloList,
        "rushYpgElo": initialEloList,
        "tdElo": initialEloList,
        "rushTdElo": initialEloList,
        "intElo": initialEloList,
        "date": initialDateList,
        "season": [0],
        "game": [0],
        "week": [0],
        "ypgPred": [0],
        "ypgAct": [0],
        "rushYpgPred": [0],
        "rushYpgAct": [0],
        "tdPred": [0],
        "tdAct": [0],
        "rushTdPred": [0],
        "rushTdAct": [0],
        "intPred": [0],
        "intAct": [0],
        "dk": [0],
        "dkAct": [0],
        "ppr" : [0],
        "pprAct" : [0],
        "fd": [0],
        "fdAct": [0],
        "std": [0],
        "stdAct": [0],
        "opp": initialOppList
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
        "date": [],
        "opp": initialOppList
    }

# Default dictionary for teams
# NOTE: See if still needed
teams_default = {
    "last": initialTeamElo,
    "elo": initialEloList,
    "lastTD": initialTeamElo,
    "eloTD": initialEloList,
    "date": initialDateList,
    "opp": initialOppList,
}

# NOTE: may not be needed (already on the playerstats)
# Demographic variables to merge onto players
demos = ["Pos"]

extension = ".csv"

playerstatroot = "fantasy-player-game-statistics"
teamstatroot = "fantasy-team-game-statistics"

# Do all the work - looping over each season
from tqdm import tqdm
tqdm.write("Beginning ComputeElo")

seasons = range(1999, 2019 + 1)
# Make the big files for players and teams -- combines all seasons except the initialization seasons from runOldSeasons
outlist_player = []
outlist_team = []
for season in seasons:
    # Read in files for the current season
    playerstatfile = Path("..", "data_raw", "fantasy", f"{playerstatroot}{season}{extension}")
    teamstatfile = Path("..", "data_raw", "fantasy", f"{teamstatroot}{season}{extension}")

    playerstats_season = eu.readplayergamestats(playerstatfile)
    teamstats_season = eu.readteamgamedata(teamstatfile, nfl=True)

    outlist_player.append(playerstats_season)
    outlist_team.append(teamstats_season)

playerstats = pd.concat(outlist_player)
teamstats = pd.concat(outlist_team)

dates = [int(x) for x in sorted(list(playerstats["gamedate"].unique()))]

# Filter based on stats/position
if processRBs:
    # RBs - Filter to only players with at least 1 Rush Attempt
    rbstats = playerstats[playerstats["Pos"].isin(["RB", "HB", "FB"])].copy()

if processWRs:
    # WRs - Filter to only players with at least 1 Catch
    wrstats = playerstats[playerstats["Pos"].isin(["WR"])].copy()

if processTEs:
    # TEs - Filter to only players with at least 1 Catch
    testats = playerstats[playerstats["Pos"].isin(["TE"])].copy()

if processQBs:
    # QBs - Filter to only players with at least 1 Pass Attempt
    qbstats = playerstats[playerstats["Pos"].isin(["QB"])].copy()

# NOTE: Tackle Solo is not a variable, so this fails in Fantasy -- okay because defense isn't analyzed player-by-player anyway
if processDefense:
    # Defense - Filter to only players with some kind of tackle
    defstats = playerstats[(playerstats["Tackle Solo"] > 0) | (playerstats["Tackle Assist"] > 0)].copy()

#### Date in dates
# Loop over each date - want to update Elo after each game
# Team Numbers to use
nTeams = 32
teamnum = list(range(1, nTeams + 1))

dates = tqdm(dates)
for date in dates:
    dates.set_description(f"d: {date}")
    # Regress team Elo on season change
    if (date - season) > 9500:
        season = date
        thisSeason = int(season / 10000)

    ##### START REGRESS
        if regress:
            ## TEAMS: Regress
            # Compute average Elo
            avgRushDelo = 0
            avgRushOelo = 0
            avgRushDeloTD = 0
            avgRushOeloTD = 0
            teamCountRush = 0

            avgPassDelo = 0
            avgPassOelo = 0
            avgPassDeloTD = 0
            avgPassOeloTD = 0
            teamCountPass = 0

            # Get sums of different Elo values
            for tm in teamnum:
                # Make sure team exists and played previous season
                if len(rushD[tm]["elo"]) > 1:
                    avgRushDelo = avgRushDelo + rushD[tm]["elo"][-1]
                    avgRushOelo = avgRushOelo + rushO[tm]["elo"][-1]
                    avgRushDeloTD = avgRushDeloTD + rushD[tm]["eloTD"][-1]
                    avgRushOeloTD = avgRushOeloTD + rushO[tm]["eloTD"][-1]
                    teamCountRush += 1
                if len(passD[tm]["elo"]) > 1:
                    avgPassDelo = avgPassDelo + passD[tm]["elo"][-1]
                    avgPassOelo = avgPassOelo + passO[tm]["elo"][-1]
                    avgPassDeloTD = avgPassDeloTD + passD[tm]["eloTD"][-1]
                    avgPassOeloTD = avgPassOeloTD + passO[tm]["eloTD"][-1]
                    teamCountPass += 1

            avgRushDelo = round(avgRushDelo / teamCountRush)
            avgRushOelo = round(avgRushOelo / teamCountRush)
            avgRushDeloTD = round(avgRushDeloTD / teamCountRush)
            avgRushOeloTD = round(avgRushOeloTD / teamCountRush)

            avgPassDelo = round(avgPassDelo / teamCountPass)
            avgPassOelo = round(avgPassOelo / teamCountPass)
            avgPassDeloTD = round(avgPassDeloTD / teamCountPass)
            avgPassOeloTD = round(avgPassOeloTD / teamCountPass)

            # Regress team Elo based on avg
            for tm in teamnum:
                # Make sure team exists and played previous season
                ## RUSH
                if len(rushD[tm]["elo"]) > 1:
                    lastElo = rushD[tm]["elo"][-1]
                    newElo = round(0.75 * lastElo + 0.25 * avgRushDelo)
                    if addRegressionDataPoint:
                        rushD[tm]["elo"] = rushD[tm]["elo"] + [newElo]
                    else: #otherwise, replace the last one
                        rushD[tm]["elo"][-1] = newElo

                    lastElo = rushD[tm]["eloTD"][-1]
                    newElo = round(0.75 * lastElo + 0.25 * avgRushDeloTD)
                    if addRegressionDataPoint:
                        rushD[tm]["eloTD"] = rushD[tm]["eloTD"] + [newElo]
                    else:
                        rushD[tm]["eloTD"][-1] = newElo

                    lastElo = rushO[tm]["elo"][-1]
                    newElo = round(0.75 * lastElo + 0.25 * avgRushOelo)
                    if addRegressionDataPoint:
                        rushO[tm]["elo"] = rushO[tm]["elo"] + [newElo]
                    else:
                        rushO[tm]["elo"][-1] = newElo

                    lastElo = rushO[tm]["eloTD"][-1]
                    newElo = round(0.75 * lastElo + 0.25 * avgRushOeloTD)
                    if addRegressionDataPoint:
                        rushO[tm]["eloTD"] = rushO[tm]["eloTD"] + [newElo]
                    else:
                        rushO[tm]["eloTD"][-1] = newElo

                    if addRegressionDataPoint:
                        # Align other arrays with the regression
                        date = rushD[tm]["date"][-1]
                        rushD[tm]["date"] = rushD[tm]["date"] + [date]
                        rushD[tm]["opp"] = rushD[tm]["opp"] + [0]
                        date = rushO[tm]["date"][-1]
                        rushO[tm]["date"] = rushO[tm]["date"] + [date]
                        rushO[tm]["opp"] = rushO[tm]["opp"] + [0]
                ## PASS
                if len(passD[tm]["elo"]) > 1:

                    lastElo = passD[tm]["elo"][-1]
                    newElo = round(0.75 * lastElo + 0.25 * avgPassDelo)
                    if addRegressionDataPoint:
                        passD[tm]["elo"] = passD[tm]["elo"] + [newElo]
                    else: #otherwise, replace the last one
                        passD[tm]["elo"][-1] = newElo

                    lastElo = passD[tm]["eloTD"][-1]
                    newElo = round(0.75 * lastElo + 0.25 * avgPassDeloTD)
                    if addRegressionDataPoint:
                        passD[tm]["eloTD"] = passD[tm]["eloTD"] + [newElo]
                    else:
                        passD[tm]["eloTD"][-1] = newElo

                    lastElo = passO[tm]["elo"][-1]
                    newElo = round(0.75 * lastElo + 0.25 * avgPassOelo)
                    if addRegressionDataPoint:
                        passO[tm]["elo"] = passO[tm]["elo"] + [newElo]
                    else:
                        passO[tm]["elo"][-1] = newElo

                    lastElo = passO[tm]["eloTD"][-1]
                    newElo = round(0.75 * lastElo + 0.25 * avgPassOeloTD)
                    if addRegressionDataPoint:
                        passO[tm]["eloTD"] = passO[tm]["eloTD"] + [newElo]
                    else:
                        passO[tm]["eloTD"][-1] = newElo

                    if addRegressionDataPoint:
                      # Align other arrays with the regression
                      date = passD[tm]["date"][-1]
                      passD[tm]["date"] = passD[tm]["date"] + [date]
                      passD[tm]["opp"] = passD[tm]["opp"] + [0]
                      date = rushO[tm]["date"][-1]
                      passO[tm]["date"] = passO[tm]["date"] + [date]
                      passO[tm]["opp"] = passO[tm]["opp"] + [0]

            ## RBs: Regress
            if processRBs:
                # Compute average Elos
                avgYpgElo = 0
                avgRecElo = 0
                avgRecYpgElo = 0
                avgTdElo = 0
                nRBs = 0

                rbList = list(rbstats[(rbstats["From"] < thisSeason) & (rbstats["To"] >= thisSeason)]["unique_id"].unique())

                if len(rbList) > 0:
                    for me_id in rbList:
                        # Initialize a player dictionary if not already present
                        if me_id not in fRBs:
                            fRBs[me_id] = deepcopy(fRBs_default)
                            fRBs[me_id]["unique_id"] = me_id
                        # Check that the RB has played
                        elif fRBs[me_id]["count"] > 0:
                            avgYpgElo = avgYpgElo + fRBs[me_id]["ypgElo"][-1]
                            avgRecElo = avgRecElo + fRBs[me_id]["recElo"][-1]
                            avgRecYpgElo = avgRecYpgElo + fRBs[me_id]["recYpgElo"][-1]
                            avgTdElo = avgTdElo + fRBs[me_id]["tdElo"][-1]
                            nRBs += 1

                    avgYpgElo = avgYpgElo / nRBs
                    avgRecElo = avgRecElo / nRBs
                    avgRecYpgElo = avgRecYpgElo / nRBs
                    avgTdElo = avgTdElo / nRBs

                    # Now regress toward avg
                    for me_id in rbList:
                        # Check that the RB has played
                        if fRBs[me_id]["count"] > 0:
                            lastElo = fRBs[me_id]["ypgElo"][-1]
                            newElo = round(0.75 * lastElo + 0.25 * avgYpgElo)
                            if addRegressionDataPoint:
                                fRBs[me_id]["ypgElo"] = fRBs[me_id]["ypgElo"] + [newElo]
                            else:
                                fRBs[me_id]["ypgElo"][-1] = newElo

                            lastElo = fRBs[me_id]["recElo"][-1]
                            newElo = round(0.75 * lastElo + 0.25 * avgRecElo)
                            if addRegressionDataPoint:
                                fRBs[me_id]["recElo"] = fRBs[me_id]["recElo"] + [newElo]
                            else:
                                fRBs[me_id]["recElo"][-1] = newElo

                            lastElo = fRBs[me_id]["recYpgElo"][-1]
                            newElo = round(0.75 * lastElo + 0.25 * avgRecYpgElo)
                            if addRegressionDataPoint:
                                fRBs[me_id]["recYpgElo"] = fRBs[me_id]["recYpgElo"] + [newElo]
                            else:
                                fRBs[me_id]["recYpgElo"][-1] = newElo

                            lastElo = fRBs[me_id]["tdElo"][-1]
                            newElo = round(0.75 * lastElo + 0.25 * avgTdElo)
                            if addRegressionDataPoint:
                                fRBs[me_id]["tdElo"] = fRBs[me_id]["tdElo"] + [newElo]
                            else:
                                fRBs[me_id]["tdElo"][-1] = newElo

                            if addRegressionDataPoint:
                                # Align all other arrays with the regression
                                date = fRBs[me_id]["date"][-1]
                                fRBs[me_id]["date"] = fRBs[me_id]["date"] + [date]
                                fRBs[me_id]["season"] = fRBs[me_id]["season"] + [thisSeason]

                                # Add a zero to all of these
                                add_zero_to = ["game", "week", "rushYdsPred", "rushYdsAct", "recYdsPred", "recYdsAct", "recPred", "recAct", "tdPred", "tdAct", "dk", "dkAct", "ppr", "pprAct", "fd", "fdAct", "std", "stdAct", "opp"]
                                for var in add_zero_to:
                                    fRBs[me_id][var] = fRBs[me_id][var] + [0]

            ## WRs: Regress
            if processWRs:
                # Compute average Elos
                avgYpgElo = 0
                avgRecElo = 0
                avgTdElo = 0
                nWRs = 0

                wrList = list(wrstats[(wrstats["From"] < thisSeason) & (wrstats["To"] >= thisSeason)]["unique_id"].unique())

                if len(wrList) > 0:
                    for me_id in wrList:
                        # Initialize a player dictionary if not already present
                        if me_id not in fWRs:
                            fWRs[me_id] = deepcopy(fWRs_default)
                            fWRs[me_id]["unique_id"] = me_id
                        # Check that WR has played
                        elif fWRs[me_id]["count"] > 0:
                            avgYpgElo = avgYpgElo + fWRs[me_id]["ypgElo"][-1]
                            avgRecElo = avgRecElo + fWRs[me_id]["recElo"][-1]
                            avgTdElo = avgTdElo + fWRs[me_id]["tdElo"][-1]
                            nWRs += 1

                    avgYpgElo = avgYpgElo / nWRs
                    avgRecElo = avgRecElo / nWRs
                    avgTdElo = avgTdElo / nWRs

                    # Now regress toward avg
                    for me_id in wrList:
                        # Check that WR has played
                        if fWRs[me_id]["count"] > 0:
                            lastElo = fWRs[me_id]["ypgElo"][-1]
                            newElo = round(0.75 * lastElo + 0.25 * avgYpgElo)
                            if addRegressionDataPoint:
                                fWRs[me_id]["ypgElo"] = fWRs[me_id]["ypgElo"] + [newElo]
                            else:
                                fWRs[me_id]["ypgElo"][-1] = newElo

                            lastElo = fWRs[me_id]["recElo"][-1]
                            newElo = round(0.75 * lastElo + 0.25 * avgRecElo)
                            if addRegressionDataPoint:
                                fWRs[me_id]["recElo"] = fWRs[me_id]["recElo"] + [newElo]
                            else:
                                fWRs[me_id]["recElo"][-1] = newElo

                            lastElo = fWRs[me_id]["tdElo"][-1]
                            newElo = round(0.75 * lastElo + 0.25 * avgTdElo)
                            if addRegressionDataPoint:
                                fWRs[me_id]["tdElo"] = fWRs[me_id]["tdElo"] + [newElo]
                            else:
                                fWRs[me_id]["tdElo"][-1] = newElo

                            if addRegressionDataPoint:
                                # Align all other arrays with the regression
                                date = fWRs[me_id]["date"][-1]
                                fWRs[me_id]["date"] = fWRs[me_id]["date"] + [date]
                                fWRs[me_id]["season"] = fWRs[me_id]["season"] + [thisSeason]

                                # Add a zero to all of these
                                add_zero_to = ["game", "week","ypgPred","ypgAct","recPred","recAct","tdPred","tdAct","dk","dkAct","ppr","pprAct","fd","fdAct","std",
                                    "stdAct", "opp"]
                                for var in add_zero_to:
                                    fWRs[me_id][var] = fWRs[me_id][var] + [0]

            ## TEs: Regress
            if processTEs:
                # Compute average Elos
                avgYpgElo = 0
                avgRecElo = 0
                avgTdElo = 0
                nTEs = 0

                teList = list(testats[(testats["From"] < thisSeason) & (testats["To"] >= thisSeason)]["unique_id"].unique())

                if len(teList) > 0:
                    for me_id in teList:
                      # Initialize a player dictionary if not already present
                      if me_id not in fTEs:
                          fTEs[me_id] = deepcopy(fTEs_default)
                          fTEs[me_id]["unique_id"] = me_id
                      # Check that TE has played
                      elif fTEs[me_id]["count"] > 0:
                          avgYpgElo = avgYpgElo + fTEs[me_id]["ypgElo"][-1]
                          avgRecElo = avgRecElo + fTEs[me_id]["recElo"][-1]
                          avgTdElo = avgTdElo + fTEs[me_id]["tdElo"][-1]
                          nTEs += 1

                    avgYpgElo = avgYpgElo / nTEs
                    avgRecElo = avgRecElo / nTEs
                    avgTdElo = avgTdElo / nTEs

                    # Now Regress toward avg
                    for me_id in teList:
                        # Check that TE has played
                        if fTEs[me_id]["count"] > 0:
                            lastElo = fTEs[me_id]["ypgElo"][-1]
                            newElo = round(0.75 * lastElo + 0.25 * avgYpgElo)
                            if addRegressionDataPoint:
                                fTEs[me_id]["ypgElo"] = fTEs[me_id]["ypgElo"] + [newElo]
                            else:
                                fTEs[me_id]["ypgElo"][-1] = newElo

                            lastElo = fTEs[me_id]["recElo"][-1]
                            newElo = round(0.75 * lastElo + 0.25 * avgRecElo)
                            if addRegressionDataPoint:
                                fTEs[me_id]["recElo"] = fTEs[me_id]["recElo"] + [newElo]
                            else:
                                fTEs[me_id]["recElo"][-1] = newElo

                            lastElo = fTEs[me_id]["tdElo"][-1]
                            newElo = round(0.75 * lastElo + 0.25 * avgTdElo)
                            if addRegressionDataPoint:
                                fTEs[me_id]["tdElo"] = fTEs[me_id]["tdElo"] + [newElo]
                            else:
                                fTEs[me_id]["tdElo"][-1] = newElo

                            if addRegressionDataPoint:
                                # Align all other arrays with the regression
                                date = fTEs[me_id]["date"][-1]
                                fTEs[me_id]["date"] = fTEs[me_id]["date"] + [date]
                                fTEs[me_id]["season"] = fTEs[me_id]["season"] + [thisSeason]

                                # Add a zero to all of these
                                add_zero_to = ["game","week","ypgPred","ypgAct","recPred","recAct","tdPred","tdAct","dk","dkAct","ppr","pprAct","fd","fdAct","std","stdAct","opp"
                                    ]
                                for var in add_zero_to:
                                    fTEs[me_id][var] = fTEs[me_id][var] + [0]

            ## QBs: Regress
            if processQBs:
                # Compute average Elos
                avgYpgElo = 0
                avgRushYpgElo = 0
                avgIntElo = 0
                avgTdElo = 0
                avgRushTdElo = 0
                nQBs = 0

                qbList = list(qbstats[(qbstats["From"] < thisSeason) & (qbstats["To"] >= thisSeason)]["unique_id"].unique())

                if len(qbList) > 0:
                    for me_id in qbList:
                        # Initialize a player dictionary if not already present
                        if me_id not in fQBs:
                            fQBs[me_id] = deepcopy(fQBs_default)
                            fQBs[me_id]["unique_id"] = me_id
                        # Check that QB has played
                        if fQBs[me_id]["count"] > 0:
                            avgYpgElo = avgYpgElo + fQBs[me_id]["ypgElo"][-1]
                            avgRushYpgElo = avgRushYpgElo + fQBs[me_id]["rushYpgElo"][-1]
                            avgIntElo = avgIntElo + fQBs[me_id]["intElo"][-1]
                            avgTdElo = avgTdElo + fQBs[me_id]["tdElo"][-1]
                            avgRushTdElo = avgRushTdElo + fQBs[me_id]["rushTdElo"][-1]
                            nQBs += 1

                    avgYpgElo = avgYpgElo / nQBs
                    avgRushYpgElo = avgRushYpgElo / nQBs
                    avgIntElo = avgIntElo / nQBs
                    avgTdElo = avgTdElo / nQBs
                    avgRushTdElo = avgRushTdElo / nQBs

                    # Now Regress toward avg
                    for me_id in qbList:
                        # Check that QB has played
                        if fQBs[me_id]["count"] > 0:
                            lastElo = fQBs[me_id]["ypgElo"][-1]
                            newElo = round(0.75 * lastElo + 0.25 * avgYpgElo)
                            if addRegressionDataPoint:
                                fQBs[me_id]["ypgElo"] = fQBs[me_id]["ypgElo"] + [newElo]
                            else:
                                fQBs[me_id]["ypgElo"][-1] = newElo

                            lastElo = fQBs[me_id]["rushYpgElo"][-1]
                            newElo = round(0.75 * lastElo + 0.25 * avgRushYpgElo)
                            if addRegressionDataPoint:
                                fQBs[me_id]["rushYpgElo"] = fQBs[me_id]["rushYpgElo"] + [newElo]
                            else:
                                fQBs[me_id]["rushYpgElo"][-1] = newElo

                            lastElo = fQBs[me_id]["intElo"][-1]
                            newElo = round(0.75 * lastElo + 0.25 * avgIntElo)
                            if addRegressionDataPoint:
                                fQBs[me_id]["intElo"] = fQBs[me_id]["intElo"] + [newElo]
                            else:
                                fQBs[me_id]["intElo"][-1] = newElo

                            lastElo = fQBs[me_id]["tdElo"][-1]
                            newElo = round(0.75 * lastElo + 0.25 * avgTdElo)
                            if addRegressionDataPoint:
                                fQBs[me_id]["tdElo"] = fQBs[me_id]["tdElo"] + [newElo]
                            else:
                                fQBs[me_id]["tdElo"][-1] = newElo

                            lastElo = fQBs[me_id]["rushTdElo"][-1]
                            newElo = round(0.75 * lastElo + 0.25 * avgRushTdElo)
                            if addRegressionDataPoint:
                                fQBs[me_id]["rushTdElo"] = fQBs[me_id]["rushTdElo"] + [newElo]
                            else:
                                fQBs[me_id]["rushTdElo"][-1] = newElo

                            if addRegressionDataPoint:
                                # Align all other arrays with the regression
                                date = fQBs[me_id]["date"][-1]
                                fQBs[me_id]["date"] = fQBs[me_id]["date"] + [date]
                                fQBs[me_id]["season"] = fQBs[me_id]["season"] + [thisSeason]

                                # Add a zero to all of these
                                add_zero_to = ["game","week","ypgPred","ypgAct","rushYpgPred","rushYpgAct","intPred","intAct","tdPred","tdAct","rushTdPred","rushTdAct","dk","dkAct","ppr","pprAct","fd","fdAct","std","stdAct","opp"
                                    ]
                                for var in add_zero_to:
                                    fQBs[me_id][var] = fQBs[me_id][var] + [0]
        # #### END REGRESS

    # Grab the players/games only on this date
    if processRBs:
        rbs = rbstats[rbstats["gamedate"] == date].to_dict("records")
    if processWRs:
        wrs = wrstats[wrstats["gamedate"] == date].to_dict("records")
    if processTEs:
        tes = testats[testats["gamedate"] == date].to_dict("records")
    if processQBs:
        qbs = qbstats[qbstats["gamedate"] == date].to_dict("records")
    if processDefense:
        defs = defstats[defstats["gamedate"] == date].to_dict("records")

    teams = teamstats[teamstats["gamedate"] == date].to_dict("records")

    # First do player evaluations - Must do this before we update the team values

    ####  RB Evaluation
    if processRBs and len(rbs) > 0:
        for rb in rbs:
            # Player's `unique_id` and opponent team's `Team Code`
            me_id = rb["unique_id"]
            them_id = rb["Team Code opp"]

            # Initialize a player dictionary if not already present
            if me_id not in fRBs:
                fRBs[me_id] = deepcopy(fRBs_default)
                fRBs[me_id]["unique_id"] = me_id
                # Add the demographic/position variables
                for demo in demos:
                    fRBs[me_id][demo] = rb[demo]

            # Save the most current Rush D Elo
            rushDelo = rushD[them_id]["elo"][-1]
            passDelo = passD[them_id]["elo"][-1]
            rushDeloTD = rushD[them_id]["eloTD"][-1]
            passDeloTD = passD[them_id]["eloTD"][-1]

            # Save the current length of the Elo array
            length = len(fRBs[me_id]["ypgElo"])

        #### RBs: Rush Yards Per Game Elo
            # Calculate point differential - in this case yards
            ptdiff = abs(rb["Rush Yard"] - rushPdrawline)

            # Win if exceeded the drawline
            win = rb["Rush Yard"] > rushPdrawline

            # Grab current YPG Elo
            ypgElo = fRBs[me_id]["ypgElo"][-1]

            # Fantasy Test
            #if length > 9:
            if True:
                if ((ypgElo > rushDelo) and win) or ((ypgElo < rushDelo) and not win):
                    rbYPGwins += 1
                else:
                    rbYPGlosses += 1

                eDiff = ypgElo - rushDelo
                ypgDiff = rb["Rush Yard"] - rushPdrawline

                if ypgDiff == 0:
                    ypgDiff = 1
                ratio = eDiff / ypgDiff

                rbRatio = rbRatio + [ratio]
                predYPG = round(rushPdrawline + (eDiff / 4))

                if predYPG < 0:
                    predYPG = 0
                error = predYPG - rb["Rush Yard"]
                rbSqError = rbSqError + (error ** 2)
                rbCount += 1
                rbError = rbError + [error]
                rbPred = rbPred + [predYPG]
                rbAct = rbAct + [rb["Rush Yard"]]
                rbEloDiff = rbEloDiff + [eDiff]

            # Compute new Elo
            Elo = eu.updateElo(ypgElo, rushDelo, ptdiff, win, blowoutFactor, playerK)

            # Update values
            fRBs[me_id]["ypgElo"] = fRBs[me_id]["ypgElo"] + [Elo]
            fRBs[me_id]["lastYPG"] = Elo

            # Update fantasy debugging values
            fRBs[me_id]["rushYdsPred"] = fRBs[me_id]["rushYdsPred"] + [predYPG]
            fRBs[me_id]["rushYdsAct"] = fRBs[me_id]["rushYdsAct"] + [rb["Rush Yard"]]

            if win:
                fRBs[me_id]["wYPG"] += 1
            else:
                fRBs[me_id]["lYPG"] += 1

        #### RBs: Rec Yards Per Game Elo
            # Compute yards from scrimmage
            recYds = rb["Rec Yards"]

            # Compute point diff
            ptdiff = abs(recYds - rushRecYdDrawline)

            # Win if exceeded drawline
            win = recYds > rushRecYdDrawline

            # Grab current recYPG Elo
            recYpgElo = fRBs[me_id]["recYpgElo"][-1]

            # Fantasy Test
            #if rb["Rush Att"] > 0:
            if True:
                if ((recYpgElo > passDelo) and win) or ((recYpgElo < passDelo) and not win):
                    rbYFSwins += 1
                else:
                    rbYFSlosses += 1

                eDiff = recYpgElo - passDelo
                recYdsDiff = rb["Rec Yards"] - rushRecYdDrawline
                if recYdsDiff == 0:
                    recYdsDiff = 1
                ratio = eDiff / recYdsDiff
                rbRecYdsRatio = rbRecYdsRatio + [ratio]

                predRecYds = round(rushRecYdDrawline + (eDiff / 4))
                if predRecYds < 0:
                    predRecYds = 0
                error = predRecYds - rb["Rec Yards"]
                rbSqErrorYFS = rbSqErrorYFS + (error ** 2)
                rbCountYFS += 1
                rbErrorYFS = rbErrorYFS + [error]
                rbPredYFS = rbPredYFS + [predRecYds]
                rbActYFS = rbActYFS + [rb["Rec Yards"]]
                yfsEloDiff = yfsEloDiff + [eDiff]
            #

            # Compute new Elo
            Elo = eu.updateElo(recYpgElo, passDelo, ptdiff, win, blowoutFactor, playerK)

            # Update values
            fRBs[me_id]["recYpgElo"] = fRBs[me_id]["recYpgElo"] + [Elo]
            fRBs[me_id]["lastYFS"] = Elo

            # Update fantasy values for debugging
            fRBs[me_id]["recYdsPred"] = fRBs[me_id]["recYdsPred"] + [predRecYds]
            fRBs[me_id]["recYdsAct"] = fRBs[me_id]["recYdsAct"] + [rb["Rec Yards"]]

            if win:
                fRBs[me_id]["wYFS"] += 1
            else:
                fRBs[me_id]["lYFS"] += 1

        #### RBs: Rec Elo
            # Sum all TDs
            rec = rb["Rec"]

            # Compute point diff
            ptdiff = abs(rec - rushRecDrawline)

            # Win if exceeds drawline
            win = rec > rushRecDrawline

            # Grab current TD Elo
            recElo = fRBs[me_id]["recElo"][-1]

            # Fantasy Test
            #if rb["Rush Att"] > 0:
            if True:
                eDiff = recElo - passDelo
                recDiff = rb["Rec"] - rushRecDrawline
                if recDiff == 0:
                    recDiff = 1
                ratio = eDiff / recDiff
                rbRecRatio = rbRecRatio + [ratio]

                predRec = floor(rushRecDrawline + (eDiff/60))
                if predRec < 0:
                    predRec = 0
                error = predRec - rb["Rec"]
                rbRecSqError = rbRecSqError + (error ** 2)
                rbRecCount = rbRecCount + 1
                rbRecError = rbRecError + [error]
                rbRecPred = rbRecPred + [predRec]
                rbRecAct = rbRecAct + [rb["Rec"]]
                rbRecEloDiff = rbRecEloDiff + [eDiff]
            #

            # Compute new Elo
            Elo = eu.updateElo(recElo, passDelo, ptdiff, win, blowoutFactor, playerK)

            # Update Values
            fRBs[me_id]["recElo"] = fRBs[me_id]["recElo"] + [Elo]

            # Update fantasy values for debugging
            fRBs[me_id]["recPred"] = fRBs[me_id]["recPred"] + [predRec]
            fRBs[me_id]["recAct"] = fRBs[me_id]["recAct"] + [rb["Rec"]]

        #### RBs: TD Elo
            # Sum all TDs
            td = rb["Rush TD"] + rb["Rec TD"] # + rb["Kickoff Ret TD"] + rb["Punt Ret TD"]

            # Compute point diff
            ptdiff = abs(td - rushTDdrawline)

            # Win if exceeds drawline
            win = td > rushTDdrawline

            # Grab current TD Elo
            tdElo = fRBs[me_id]["tdElo"][-1]

            # Fantasy Test
            #if rb["Rush Att"] > 0:
            if True:
                if((tdElo > rushDeloTD) and win) or ((tdElo < rushDeloTD) and not win):
                    rbTDwins += 1
                else:
                    rbTDlosses += 1

                eDiff = tdElo - rushDeloTD
                tdDiff = rb["Rush TD"] + rb["Rec TD"] - rushTDdrawline
                if tdDiff == 0:
                    tdDiff = 1
                ratio = eDiff / tdDiff
                rbTDRatio = rbTDRatio + [ratio]

                predTD = floor(rushTDdrawline + 1 + (eDiff/100)) # Hardcoded 1 here
                if predTD < 0:
                    predTD = 0
                error = predTD - (rb["Rush TD"] + rb["Rec TD"])
                rbTDSqError = rbTDSqError + (error ** 2)
                rbTDCount += 1
                rbTDError = rbTDError + [error]
                rbTDPred = rbTDPred + [predTD]
                rbTDAct = rbTDAct + [rb["Rush TD"] + rb["Rec TD"]]
                rbTDEloDiff = rbTDEloDiff + [eDiff]
            #

            # Compute new Elo
            Elo = eu.updateElo(tdElo, rushDeloTD, ptdiff, win, blowoutFactor, playerK)

            # Update Values
            fRBs[me_id]["tdElo"] = fRBs[me_id]["tdElo"] + [Elo]
            fRBs[me_id]["lastTD"] = Elo

            # Update values to debug fantasy
            fRBs[me_id]["tdPred"] = fRBs[me_id]["tdPred"] + [predTD]
            fRBs[me_id]["tdAct"] = fRBs[me_id]["tdAct"] + [rb["Rush TD"] + rb["Rec TD"]]

            if win:
                fRBs[me_id]["wTD"] += 1
            else:
                fRBs[me_id]["lTD"] += 1

        #### RBs: Dates & Cumulative Elo
            # Add the date (if first time, add a date for the first date)
            if fRBs[me_id]["count"] == 0:
                fRBs[me_id]["date"][0] = eu.get_start_date(date)
            fRBs[me_id]["date"] = fRBs[me_id]["date"] + [date]

            # Add season, game, week
            fRBs[me_id]["season"] = fRBs[me_id]["season"] + [thisSeason]
            fRBs[me_id]["game"] = fRBs[me_id]["game"] + [rb["G"]]
            fRBs[me_id]["week"] = fRBs[me_id]["week"] + [rb["Week"]]

            # Add the opponent
            fRBs[me_id]["opp"] = fRBs[me_id]["opp"] + [rb["Team Code opp"]]

            # Count # of games the player has data for
            fRBs[me_id]["count"] += 1

        #### RBs: Fantasy points
            #
            # Draft Kings
            #
            dk = (0.1 * predYPG) + (0.1 * predRecYds) + (1.0 * predRec) + (6.0 * predTD)
            if predYPG > 100:
                dk += 3

            if predRecYds > 100:
                dk += 3

            fRBs[me_id]["dk"] = fRBs[me_id]["dk"] + [dk]
            fRBs[me_id]["dkAct"] = fRBs[me_id]["dkAct"] + [rb["DK Pts"]]

            #
            # PPR
            #
            ppr = (0.1 * predYPG) + (0.1 * predRecYds) + (1.0 * predRec) + (6.0 * predTD)

            fRBs[me_id]["ppr"] = fRBs[me_id]["ppr"] + [ppr]
            fRBs[me_id]["pprAct"] = fRBs[me_id]["pprAct"] + [rb["PPR"]]

            #
            # STD
            #
            std = (0.1 * predYPG) + (0.1 * predRecYds) + (6.0 * predTD)

            fRBs[me_id]["std"] = fRBs[me_id]["std"] + [std]
            fRBs[me_id]["stdAct"] = fRBs[me_id]["stdAct"] + [rb["Fantasy Pts"]]

            #
            # Fan Duel / Yahoo / Half PPR
            #
            fd = (0.1 * predYPG) + (0.1 * predRecYds) + (0.5 * predRec) + (6.0 * predTD)

            fRBs[me_id]["fd"] = fRBs[me_id]["fd"] + [fd]
            fRBs[me_id]["fdAct"] = fRBs[me_id]["fdAct"] + [rb["FD Pts"]]

            # Test RMS Error
            if length > 9:
                error = dk - rb["DK Pts"]
                rbDKSqError = rbDKSqError + (error ** 2)
                rbDKCount += 1
                rbDKError = rbDKError + [error]
                rbDKPred = rbDKPred + [dk]
                rbDKAct = rbDKAct + [rb["DK Pts"]]

    #### WR Evaluation
    if processWRs and len(wrs) > 0:
        for wr in wrs:
            # Player's `unique_id` and opponent team's `Team Code`
            me_id = wr["unique_id"]
            them_id = wr["Team Code opp"]

            # Initialize a player dictionary if not already present
            if me_id not in fWRs:
                fWRs[me_id] = deepcopy(fWRs_default)
                fWRs[me_id]["unique_id"] = me_id
                # Add the demographic/position variables
                for demo in demos:
                    fWRs[me_id][demo] = wr[demo]

            # Save the most current Pass D Elo
            passDelo = passD[them_id]["elo"][-1]
            passDeloTD = passD[them_id]["eloTD"][-1]

            # Save the current len of the Elo array
            length = len(fWRs[me_id]["ypgElo"])

        #### WRs: Receptions Per Game Elo
            # Calculate point differential - in this case yards
            ptdiff = abs(wr["Rec"] - recDrawline)

            # Win if exceeded the drawline
            win = wr["Rec"] > recDrawline

            # Grab current YPG Elo
            recElo = fWRs[me_id]["recElo"][-1]

            # Fantasy Test
            #if wr["Rec"] > 0:
            if True:
                eDiff = recElo - passDelo
                recDiff = wr["Rec"] - recDrawline
                if recDiff == 0:
                    recDiff = 1
                ratio = eDiff / recDiff
                wrRecRatio = wrRecRatio + [ratio]

                predRec = floor(recDrawline + (eDiff/60))
                if predRec < 0:
                    predRec = 0
                error = predRec - wr["Rec"]
                wrRecSqError = wrRecSqError + (error**2)
                wrRecCount += 1
                wrRecError = wrRecError + [error]
                wrRecPred = wrRecPred + [predRec]
                wrRecAct = wrRecAct + [wr["Rec"]]
                wrRecEloDiff = wrRecEloDiff + [eDiff]
            #

            # Compute new Elo
            Elo = eu.updateElo(recElo, passDelo, ptdiff, win, blowoutFactor, playerK)

            # Update values
            fWRs[me_id]["recElo"] = fWRs[me_id]["recElo"] + [Elo]
            fWRs[me_id]["lastRec"] = Elo

            # Update values for fantasy debugging
            fWRs[me_id]["recPred"] = fWRs[me_id]["recPred"] + [predRec]
            fWRs[me_id]["recAct"] = fWRs[me_id]["recAct"] + [wr["Rec"]]

            if win:
                fWRs[me_id]["wRec"] += 1
            else:
                fWRs[me_id]["lRec"] += 1

        #### WRs: Receiving Yards Per Game Elo
            # Calculate point differential - in this case yards
            ptdiff = abs(wr["Rec Yards"] - recPdrawline)

            # Win if exceeded the drawline
            win = wr["Rec Yards"] > recPdrawline

            # Grab current YPG Elo
            ypgElo = fWRs[me_id]["ypgElo"][-1]

            # Fantasy test
            #if wr["Rec"] > 0:
            if True:
                if ((ypgElo > passDelo) and win) or ((ypgElo < passDelo) and not win):
                    wrYPGwins += 1
                else:
                    wrYPGlosses += 1

                eDiff = ypgElo - passDelo
                ypgDiff = wr["Rec Yards"] - recPdrawline
                if ypgDiff == 0:
                    ypgDiff = 1
                ratio = eDiff / ypgDiff
                wrRatio = wrRatio + [ratio]

                predYPG = round(recPdrawline + (eDiff / 16))
                error = predYPG - wr["Rec Yards"]
                wrYpgSqError = wrYpgSqError + (error ** 2)
                wrYpgCount += 1
                wrYpgError = wrYpgError + [error]
                wrYpgPred = wrYpgPred + [predYPG]
                wrYpgAct = wrYpgAct + [wr["Rec Yards"]]
                wrYpgEloDiff = wrYpgEloDiff + [eDiff]
            #

            # Compute new Elo
            Elo = eu.updateElo(ypgElo, passDelo, ptdiff, win, blowoutFactor, playerK)

            # Update values
            fWRs[me_id]["ypgElo"] = fWRs[me_id]["ypgElo"] + [Elo]
            fWRs[me_id]["lastYPG"] = Elo

            # Update values for debugging fantasy
            fWRs[me_id]["ypgPred"] = fWRs[me_id]["ypgPred"] + [predYPG]
            fWRs[me_id]["ypgAct"] = fWRs[me_id]["ypgAct"] + [wr["Rec Yards"]]

            if win:
                fWRs[me_id]["wYPG"] += 1
            else:
                fWRs[me_id]["lYPG"] += 1

        #### WRs: TD Elo
            # Sum all TDs
            td = wr["Rec TD"] # + wr["Kickoff Ret TD"] + wr["Punt Ret TD"]

            # Compute point diff
            ptdiff = abs(td - recTDdrawline)

            # Win if exceeds drawline
            win = td > recTDdrawline

            # Grab current TD Elo
            tdElo = fWRs[me_id]["tdElo"][-1]

            # Fantasy Test
            #if wr["Rec"] > 0:
            if True:
                if ((tdElo > passDeloTD) and  win) or ((tdElo < passDeloTD) and not win):
                    wrTDwins += 1
                else:
                    wrTDlosses += 1

                eDiff = tdElo - passDeloTD
                tdDiff = wr["Rush TD"] + wr["Rec TD"] - recTDdrawline
                if tdDiff == 0:
                    tdDiff = 1
                ratio = eDiff / tdDiff
                wrTDRatio = wrTDRatio + [ratio]

                predTD = floor(recTDdrawline + (eDiff/100))
                if predTD < 0:
                    predTD = 0
                error = predTD - (wr["Rush TD"] + wr["Rec TD"])
                wrTDSqError = wrTDSqError + (error ** 2)
                wrTDCount += 1
                wrTDError = wrTDError + [error]
                wrTDPred = wrTDPred + [predTD]
                wrTDAct = wrTDAct + [wr["Rush TD"] + wr["Rec TD"]]
                wrTDEloDiff = wrTDEloDiff + [eDiff]
            #

            # Compute new Elo
            Elo = eu.updateElo(tdElo, passDeloTD, ptdiff, win, blowoutFactor, playerK)

            # Update Values
            fWRs[me_id]["tdElo"] = fWRs[me_id]["tdElo"] + [Elo]
            fWRs[me_id]["lastTD"] = Elo

            # Update values for debugging fantasy
            fWRs[me_id]["tdPred"] = fWRs[me_id]["tdPred"] + [predTD]
            fWRs[me_id]["tdAct"] = fWRs[me_id]["tdAct"] + [wr["Rush TD"] + wr["Rec TD"]]

            if win:
                fWRs[me_id]["wTD"] += 1
            else:
                fWRs[me_id]["lTD"] += 1

        #### WRs: Dates
            # Accumulate Elo scores into a cumulative Elo
            # recElo = fWRs[me_id]["recElo"][-1]
            # ypgElo = fWRs[me_id]["ypgElo"][-1]
            # # ypcElo = fWRs[me_id]["ypcElo"][-1]
            # tdElo = fWRs[me_id]["tdElo"][-1]

            # Compute Composite Elo
            # value = recElo + ypgElo + tdElo

            # Update values
            # fWRs[me_id]["eloC"] = fWRs[me_id]["eloC"] + [value]
            # fWRs[me_id]["last"] = value

            # Add the date (if first time, add a date for the first date)
            if fWRs[me_id]["count"] == 0:
                fWRs[me_id]["date"][0] = eu.get_start_date(date)
            fWRs[me_id]["date"] = fWRs[me_id]["date"] + [date]

            # Add season, game, week
            fWRs[me_id]["season"] = fWRs[me_id]["season"] + [thisSeason]
            fWRs[me_id]["game"] = fWRs[me_id]["game"] + [wr["G"]]
            fWRs[me_id]["week"] = fWRs[me_id]["week"] + [wr["Week"]]

            # Add the opponent
            fWRs[me_id]["opp"] = fWRs[me_id]["opp"] + [wr["Team Code opp"]]

            # Count # of games the player has data for
            fWRs[me_id]["count"] += 1

        #### WRs: Fantasy Points
            #
            # Draft Kings
            #
            dk = (0.1 * predYPG) + (1.0 * predRec) + (6.0 * predTD)
            if predYPG > 100:
                dk += 3

            fWRs[me_id]["dk"] = fWRs[me_id]["dk"] + [dk]
            fWRs[me_id]["dkAct"] = fWRs[me_id]["dkAct"] + [wr["DK Pts"]]

            #
            # PPR
            #
            ppr = (0.1 * predYPG) + (1.0 * predRec) + (6.0 * predTD)

            fWRs[me_id]["ppr"] = fWRs[me_id]["ppr"] + [ppr]
            fWRs[me_id]["pprAct"] = fWRs[me_id]["pprAct"] + [wr["PPR"]]

            #
            # STD
            #
            std = (0.1 * predYPG) + (6.0 * predTD)

            fWRs[me_id]["std"] = fWRs[me_id]["std"] + [std]
            fWRs[me_id]["stdAct"] = fWRs[me_id]["stdAct"] + [wr["Fantasy Pts"]]

            #
            # Fan Duel / Yahoo / Half PPR
            #
            fd = (0.1 * predYPG) + (0.5 * predRec) + (6.0 * predTD)

            fWRs[me_id]["fd"] = fWRs[me_id]["fd"] + [fd]
            fWRs[me_id]["fdAct"] = fWRs[me_id]["fdAct"] + [wr["FD Pts"]]

            # Test RMS Error
            if length > 9:
                error = dk - wr["DK Pts"]
                wrDKSqError = wrDKSqError + (error**2)
                wrDKCount += 1
                wrDKError = wrDKError + [error]
                wrDKPred = wrDKPred + [dk]
                wrDKAct = wrDKAct + [wr["DK Pts"]]

    #### TE Evaluation
    if processTEs and len(tes) > 0:
        for te in tes:
            # Player's `unique_id` and opponent team's `Team Code`
            me_id = te["unique_id"]
            them_id = te["Team Code opp"]

            # Initialize a player dictionary if not already present
            if me_id not in fTEs:
                fTEs[me_id] = deepcopy(fTEs_default)
                fTEs[me_id]["unique_id"] = me_id
                # Add the demographic/position variables
                for demo in demos:
                    fTEs[me_id][demo] = te[demo]

            # Save the most current Pass D Elo
            passDelo = passD[them_id]["elo"][-1]
            passDeloTD = passD[them_id]["eloTD"][-1]

            # Save the current len of the Elo array
            length = len(fTEs[me_id]["ypgElo"])

        #### TEs: Receptions Per Game Elo
            # Calculate point differential - in this case yards
            ptdiff = abs(te["Rec"] - teRecDrawline)

            # Win if exceeded the drawline
            win = te["Rec"] > teRecDrawline

            # Grab current YPG Elo
            recElo = fTEs[me_id]["recElo"][-1]

            # Fantasy Test
            #if te["Rec"] > 0:
            if True:
                eDiff = recElo - passDelo
                recDiff = te["Rec"] - teRecDrawline
                if recDiff == 0:
                    recDiff = 1
                ratio = eDiff / recDiff
                teRecRatio = teRecRatio + [ratio]

                predRec = floor(teRecDrawline + (eDiff/60))
                if predRec < 0:
                    predRec = 0
                error = predRec - te["Rec"]
                teRecSqError = teRecSqError + (error**2)
                teRecCount += 1
                teRecError = teRecError + [error]
                teRecPred = teRecPred + [predRec]
                teRecAct = teRecAct + [te["Rec"]]
                teRecEloDiff = teRecEloDiff + [eDiff]
            #

            # Compute new Elo
            Elo = eu.updateElo(recElo, passDelo, ptdiff, win, blowoutFactor, playerK)

            # Update values
            fTEs[me_id]["recElo"] = fTEs[me_id]["recElo"] + [Elo]
            fTEs[me_id]["lastRec"] = Elo

            # Update values for fantasy debugging
            fTEs[me_id]["recPred"] = fTEs[me_id]["recPred"] + [predRec]
            fTEs[me_id]["recAct"] = fTEs[me_id]["recAct"] + [te["Rec"]]

            if win:
                fTEs[me_id]["wRec"] += 1
            else:
                fTEs[me_id]["lRec"] += 1

        #### TEs: Receiving Yards Per Game Elo
            # Calculate point differential - in this case yards
            ptdiff = abs(te["Rec Yards"] - teYpgdrawline)

            # Win if exceeded the drawline
            win = te["Rec Yards"] > teYpgdrawline

            # Grab current YPG Elo
            ypgElo = fTEs[me_id]["ypgElo"][-1]

            # Fantasy test
            #if te["Rec"] > 0:
            if True:
                if ((ypgElo > passDelo) and win) or ((ypgElo < passDelo) and not win):
                    teYPGwins += 1
                else:
                    teYPGlosses += 1

                eDiff = ypgElo - passDelo
                ypgDiff = te["Rec Yards"] - teYpgdrawline
                if ypgDiff == 0:
                    ypgDiff = 1
                ratio = eDiff / ypgDiff
                teYpgRatio = teYpgRatio + [ratio]

                predYPG = round(teYpgdrawline + (eDiff / 16))
                error = predYPG - te["Rec Yards"]
                teYpgSqError = teYpgSqError + (error**2)
                teYpgCount = teYpgCount + 1
                teYpgError = teYpgError + [error]
                teYpgPred = teYpgPred + [predYPG]
                teYpgAct = teYpgAct + [te["Rec Yards"]]
                teYpgEloDiff = teYpgEloDiff + [eDiff]
            #

            # Compute new Elo
            Elo = eu.updateElo(ypgElo, passDelo, ptdiff, win, blowoutFactor, playerK)

            # Update values
            fTEs[me_id]["ypgElo"] = fTEs[me_id]["ypgElo"] + [Elo]
            fTEs[me_id]["lastYPG"] = Elo

            # Update values for debugging fantasy
            fTEs[me_id]["ypgPred"] = fTEs[me_id]["ypgPred"] + [predYPG]
            fTEs[me_id]["ypgAct"] = fTEs[me_id]["ypgAct"] + [te["Rec Yards"]]

            if win:
                fTEs[me_id]["wYPG"] += 1
            else:
                fTEs[me_id]["lYPG"] += 1

        #### TEs: TD Elo
            # Sum all TDs
            td = te["Rec TD"] # + te["Kickoff Ret TD"] + te["Punt Ret TD"]

            # Compute point diff
            ptdiff = abs(td - teTDdrawline)

            # Win if exceeds drawline
            win = td > teTDdrawline

            # Grab current TD Elo
            tdElo = fTEs[me_id]["tdElo"][-1]

            # Fantasy Test
            #if wr["Rec"] > 0:
            if True:
                if ((tdElo > passDeloTD) and win) or ((tdElo < passDeloTD) and not win):
                    teTDwins += 1
                else:
                    teTDlosses += 1

                eDiff = tdElo - passDeloTD
                tdDiff = te["Rec TD"] - teTDdrawline
                if tdDiff == 0:
                    tdDiff = 1
                ratio = eDiff / tdDiff
                teTDRatio = teTDRatio + [ratio]

                predTD = floor(teTDdrawline + (eDiff/100))
                if predTD < 0:
                    predTD = 0
                error = predTD - te["Rec TD"]
                teTDSqError = teTDSqError + (error**2)
                teTDCount += 1
                teTDError = teTDError + [error]
                teTDPred = teTDPred + [predTD]
                teTDAct = teTDAct + [te["Rec TD"]]
                teTDEloDiff = teTDEloDiff + [eDiff]
            #

            # Compute new Elo
            Elo = eu.updateElo(tdElo, passDeloTD, ptdiff, win, blowoutFactor, playerK)

            # Update Values
            fTEs[me_id]["tdElo"] = fTEs[me_id]["tdElo"] + [Elo]
            fTEs[me_id]["lastTD"] = Elo

            # Update values for debugging fantasy
            fTEs[me_id]["tdPred"] = fTEs[me_id]["tdPred"] + [predTD]
            fTEs[me_id]["tdAct"] = fTEs[me_id]["tdAct"] + [te["Rush TD"] + te["Rec TD"]]

            if win:
                fTEs[me_id]["wTD"] += 1
            else:
                fTEs[me_id]["lTD"] += 1

        #### TEs: Dates
            # Add the date (if first time, add a date for the first date)
            if fTEs[me_id]["count"] == 0:
                fTEs[me_id]["date"][0] = eu.get_start_date(date)
            fTEs[me_id]["date"] = fTEs[me_id]["date"] + [date]

            # Add season, game, week
            fTEs[me_id]["season"] = fTEs[me_id]["season"] + [thisSeason]
            fTEs[me_id]["game"] = fTEs[me_id]["game"] + [te["G"]]
            fTEs[me_id]["week"] = fTEs[me_id]["week"] + [te["Week"]]

            # Add the opponent
            fTEs[me_id]["opp"] = fTEs[me_id]["opp"] + [te["Team Code opp"]]

            # Count # of games the player has data for
            fTEs[me_id]["count"] += 1

        #### TEs: Fantasy points
            #
            # Draft Kings
            #
            dk = (0.1 * predYPG) + (1.0 * predRec) + (6.0 * predTD)
            if predYPG > 100:
                dk += 3

            fTEs[me_id]["dk"] = fTEs[me_id]["dk"] + [dk]
            fTEs[me_id]["dkAct"] = fTEs[me_id]["dkAct"] + [te["DK Pts"]]

            #
            # PPR
            #
            ppr = (0.1 * predYPG) + (1.0 * predRec) + (6.0 * predTD)

            fTEs[me_id]["ppr"] = fTEs[me_id]["ppr"] + [ppr]
            fTEs[me_id]["pprAct"] = fTEs[me_id]["pprAct"] + [te["PPR"]]

            #
            # STD
            #
            std = (0.1 * predYPG) + (6.0 * predTD)

            fTEs[me_id]["std"] = fTEs[me_id]["std"] + [std]
            fTEs[me_id]["stdAct"] = fTEs[me_id]["stdAct"] + [te["Fantasy Pts"]]

            #
            # Fan Duel / Yahoo / Half PPR
            #
            fd = (0.1 * predYPG) + (0.5 * predRec) + (6.0 * predTD)

            fTEs[me_id]["fd"] = fTEs[me_id]["fd"] + [fd]
            fTEs[me_id]["fdAct"] = fTEs[me_id]["fdAct"] + [te["FD Pts"]]

            # Test RMS Error
            if length > 9:
                error = dk - te["DK Pts"]
                teDKSqError = teDKSqError + (error**2)
                teDKCount += 1
                teDKError = teDKError + [error]
                teDKPred = teDKPred + [dk]
                teDKAct = teDKAct + [te["DK Pts"]]

    #### QB Evaluation
    if processQBs and len(qbs) > 0:
        for qb in qbs:
            # Player's `unique_id` and opponent team's `Team Code`
            me_id = qb["unique_id"]
            them_id = qb["Team Code opp"]

            # Initialize a player dictionary if not already present
            if me_id not in fQBs:
                fQBs[me_id] = deepcopy(fQBs_default)
                fQBs[me_id]["unique_id"] = me_id
                # Add the demographic/position variables
                for demo in demos:
                    fQBs[me_id][demo] = qb[demo]

            # Save the most current Pass D Elo
            passDelo = passD[them_id]["elo"][-1]
            rushDelo = rushD[them_id]["elo"][-1]
            passDeloTD = passD[them_id]["eloTD"][-1]
            rushDeloTD = rushD[them_id]["eloTD"][-1]

            # Save the current length of the Elo array
            length = len(fQBs[me_id]["ypgElo"])

        #### QBs: Pass Yards Per Game Elo
            # Pass Yards
            passYards = qb["Pass Yard"]

            # Compute point diff
            ptdiff = abs(passYards - passDrawline)

            # Win if exceeds drawline
            win = passYards > passDrawline

            # Grab current YPG Elo
            ypgElo = fQBs[me_id]["ypgElo"][-1]

            # Fantasy test
            #if qb["Pass Att"] > 9:
            if True:
                # Fantasy test
                if ((ypgElo > passDelo) and win) or ((ypgElo < passDelo) and not win):
                    qbYPGwins += 1
                else:
                    qbYPGlosses += 1

                eDiff = ypgElo - passDelo
                ypgDiff = qb["Pass Yard"] - passDrawline
                if ypgDiff == 0:
                    ypgDiff = 1
                ratio = eDiff / ypgDiff
                qbYpgRatio = qbYpgRatio + [ratio]

                predYPG = round(passDrawline + (eDiff / 2))
                if predYPG < 0:
                    predYPG = 0
                error = predYPG - qb["Pass Yard"]
                qbYpgSqError = qbYpgSqError + (error**2)
                qbYpgCount += 1
                qbYpgError = qbYpgError + [error]
                qbYpgPred = qbYpgPred + [predYPG]
                qbYpgAct = qbYpgAct + [qb["Pass Yard"]]
                qbYpgEloDiff = qbYpgEloDiff + [eDiff]
            #

            # Compute new Elo
            Elo = eu.updateElo(ypgElo, passDelo, ptdiff, win, blowoutFactor, playerK)

            # Update Values
            fQBs[me_id]["ypgElo"] = fQBs[me_id]["ypgElo"] + [Elo]
            fQBs[me_id]["lastYPG"] = Elo

            # Update values to debug fantasy points
            fQBs[me_id]["ypgPred"] = fQBs[me_id]["ypgPred"] + [predYPG]
            fQBs[me_id]["ypgAct"] = fQBs[me_id]["ypgAct"] + [qb["Pass Yard"]]

            if win:
                fQBs[me_id]["wYPG"] += 1
            else:
                fQBs[me_id]["lYPG"] += 1

        #### QBs: Rush Yards Per Game Elo
            # Rush Yards
            rushYards = qb["Rush Yard"]

            # Compute point diff
            ptdiff = abs(rushYards - qbRushDrawline)

            # Win if exceeds drawline
            win = rushYards > qbRushDrawline

            # Grab current YPG Elo
            rushYpgElo = fQBs[me_id]["rushYpgElo"][-1]

            # Fantasy test
            #if qb["Pass Att"] > 9:
            if True:
                # Fantasy test
                if ((rushYpgElo > rushDelo) and win) or ((rushYpgElo < rushDelo) and not win):
                    qbRushYPGwins += 1
                else:
                    qbRushYPGlosses += 1

                eDiff = rushYpgElo - rushDelo
                ypgDiff = qb["Rush Yard"] - qbRushDrawline
                if ypgDiff == 0:
                    ypgDiff = 1
                ratio = eDiff / ypgDiff
                qbRushYpgRatio = qbRushYpgRatio + [ratio]

                predRushYPG = round(qbRushDrawline + (eDiff / 12))
                if predRushYPG < -5:
                    predRushYPG = -5
                error = predRushYPG - qb["Rush Yard"]
                qbRushYpgSqError = qbRushYpgSqError + (error**2)
                qbRushYpgCount += 1
                qbRushYpgError = qbRushYpgError + [error]
                qbRushYpgPred = qbRushYpgPred + [predRushYPG]
                qbRushYpgAct = qbRushYpgAct + [qb["Rush Yard"]]
                qbRushYpgEloDiff = qbRushYpgEloDiff + [eDiff]
            #

            # Compute new Elo
            Elo = eu.updateElo(rushYpgElo, rushDelo, ptdiff, win, blowoutFactor, playerK)

            # Update Values
            fQBs[me_id]["rushYpgElo"] = fQBs[me_id]["rushYpgElo"] + [Elo]
            fQBs[me_id]["lastRushYPG"] = Elo

            # Update values to debug fantasy points
            fQBs[me_id]["rushYpgPred"] = fQBs[me_id]["rushYpgPred"] + [predRushYPG]
            fQBs[me_id]["rushYpgAct"] = fQBs[me_id]["rushYpgAct"] + [qb["Rush Yard"]]

            if win:
              fQBs[me_id]["wRushYPG"] += 1
            else:
              fQBs[me_id]["lRushYPG"] += 1

        #### QBs: Pass TD Elo
            # Compute Pass TD
            td = qb["Pass TD"]

            # Compute point diff
            ptdiff = abs(td - passTDline)

            # Win if exceeds drawline
            win = td > passTDline

            # Grab current TD Elo
            tdElo = fQBs[me_id]["tdElo"][-1]

            # Fantasy Test
            #if qb["Pass Att"] > 0:
            if True:
                if ((tdElo > passDeloTD) and win) or ((tdElo < passDeloTD) and not win):
                    qbTDwins += 1
                else:
                    qbTDlosses += 1

                eDiff = tdElo - passDeloTD
                tdDiff = qb["Pass TD"] - passTDline
                if tdDiff == 0:
                    tdDiff = 1
                ratio = eDiff / tdDiff
                qbTDRatio = qbTDRatio + [ratio]

                predTD = floor(passTDline + (eDiff/120))
                if predTD < 0:
                    predTD = 0
                error = predTD - qb["Pass TD"]
                qbTDSqError = qbTDSqError + (error**2)
                qbTDCount += 1
                qbTDError = qbTDError + [error]
                qbTDPred = qbTDPred + [predTD]
                qbTDAct = qbTDAct + [qb["Pass TD"]]
                qbTDEloDiff = qbTDEloDiff + [eDiff]
            #

            # Compute new Elo
            Elo = eu.updateElo(tdElo, passDeloTD, ptdiff, win, blowoutFactor, playerK)

            # Update values
            fQBs[me_id]["tdElo"] = fQBs[me_id]["tdElo"] + [Elo]
            fQBs[me_id]["lastTD"] = Elo

            # Update values to debug fantasy points
            fQBs[me_id]["tdPred"] = fQBs[me_id]["tdPred"] + [predTD]
            fQBs[me_id]["tdAct"] = fQBs[me_id]["tdAct"] + [qb["Pass TD"]]

            if win:
                fQBs[me_id]["wTD"] += 1
            else:
                fQBs[me_id]["lTD"] += 1

        #### QBs: Rush TD Elo
            # Compute Pass TD
            td = qb["Rush TD"]

            # Compute point diff
            ptdiff = abs(td - qbRushTDline)

            # Win if exceeds drawline
            win = td > qbRushTDline

            # Grab current TD Elo
            tdElo = fQBs[me_id]["rushTdElo"][-1]

            # Fantasy Test
            #if qb["Pass Att"] > 0:
            if True:
                if ((tdElo > rushDeloTD) and win) or ((tdElo < rushDeloTD) and not win):
                    qbRushTDwins += 1
                else:
                    qbRushTDlosses += 1

                eDiff = tdElo - rushDeloTD
                tdDiff = qb["Rush TD"] - qbRushTDline
                if tdDiff == 0:
                    tdDiff = 1
                ratio = eDiff / tdDiff
                qbRushTDRatio = qbRushTDRatio + [ratio]

                predRushTD = floor(qbRushTDline + (eDiff/150))
                if predRushTD < 0:
                    predRushTD = 0
                error = predRushTD - qb["Rush TD"]
                qbRushTDSqError = qbRushTDSqError + (error**2)
                qbRushTDCount += 1
                qbRushTDError = qbRushTDError + [error]
                qbRushTDPred = qbRushTDPred + [predRushTD]
                qbRushTDAct = qbRushTDAct + [qb["Rush TD"]]
                qbRushTDEloDiff = qbRushTDEloDiff + [eDiff]
            #

            # Compute new Elo
            Elo = eu.updateElo(tdElo, rushDeloTD, ptdiff, win, blowoutFactor, playerK)

            # Update values
            fQBs[me_id]["rushTdElo"] = fQBs[me_id]["rushTdElo"] + [Elo]
            fQBs[me_id]["lastRushTD"] = Elo

            # Update values to debug fantasy points
            fQBs[me_id]["rushTdPred"] = fQBs[me_id]["rushTdPred"] + [predRushTD]
            fQBs[me_id]["rushTdAct"] = fQBs[me_id]["rushTdAct"] + [qb["Rush TD"]]

            if win:
                fQBs[me_id]["wRushTD"] += 1
            else:
                fQBs[me_id]["lRushTD"] += 1

        #### QBs: Pass Int Elo
            # Compute Pass Int
            intr = qb["Pass Int"]

            # Compute point diff
            ptdiff = abs(intr - passIntDrawline)

            # Win if exceeds drawline
            win = intr < passIntDrawline

            # Grab current Int Elo
            intElo = fQBs[me_id]["intElo"][-1]

            # Fantasy Test
            #if qb["Pass Att"] > 0:
            if True:
                if ((intElo > passDelo) and win) or ((intElo < passDelo) and not win):
                    qbINTwins += 1
                else:
                    qbINTlosses += 1

                eDiff = intElo - passDelo
                intDiff = passIntDrawline - qb["Pass Int"]
                if intDiff == 0:
                    intDiff = 1
                ratio = eDiff / intDiff
                qbIntRatio = qbIntRatio + [ratio]

                predInt = floor(passIntDrawline - (eDiff/100))
                if predInt < 0:
                    predInt = 0
                error = predInt - qb["Pass Int"]
                qbIntSqError = qbIntSqError + (error**2)
                qbIntCount += 1
                qbIntError = qbIntError + [error]
                qbIntPred = qbIntPred + [predInt]
                qbIntAct = qbIntAct + [qb["Pass Int"]]
                qbIntEloDiff = qbIntEloDiff + [eDiff]
            #

            # Compute new Elo
            Elo = eu.updateElo(intElo, passDelo, ptdiff, win, blowoutFactor, playerK)

            # Update values
            fQBs[me_id]["intElo"] = fQBs[me_id]["intElo"] + [Elo]
            fQBs[me_id]["lastInt"] = Elo

            # Update values for fantasy points
            fQBs[me_id]["intPred"] = fQBs[me_id]["intPred"] + [predInt]
            fQBs[me_id]["intAct"] = fQBs[me_id]["intAct"] + [qb["Pass Int"]]

            if win:
                fQBs[me_id]["wINT"] += 1
            else:
                fQBs[me_id]["lINT"] += 1

        #### QBs: Dates
            # Add the date (if first time, add a date for the first date)
            if fQBs[me_id]["count"] == 0:
                fQBs[me_id]["date"][0] = eu.get_start_date(date)
            fQBs[me_id]["date"] = fQBs[me_id]["date"] + [date]

            # Add season, game, week
            fQBs[me_id]["season"] = fQBs[me_id]["season"] + [thisSeason]
            fQBs[me_id]["game"] = fQBs[me_id]["game"] + [qb["G"]]
            fQBs[me_id]["week"] = fQBs[me_id]["week"] + [qb["Week"]]

            # Add the opponent
            fQBs[me_id]["opp"] = fQBs[me_id]["opp"] + [qb["Team Code opp"]]

            # Count # of games the player has data for
            fQBs[me_id]["count"] += 1

        #### QBs: Fantasy points
            #
            # Draft Kings
            #
            dk = (0.04 * predYPG) + (0.1 * predRushYPG) + (4.0 * predTD) + (6.0 * predRushTD) - (1.0 * predInt)
            if predYPG > 300:
                dk += 3

            if predRushYPG > 100:
                dk += 3

            fQBs[me_id]["dk"] = fQBs[me_id]["dk"] + [dk]
            fQBs[me_id]["dkAct"] = fQBs[me_id]["dkAct"] + [qb["DK Pts"]]

            #
            # PPR
            #
            ppr = (0.04 * predYPG) + (0.1 * predRushYPG) + (4.0 * predTD) + (6.0 * predRushTD) - (1.0 * predInt)

            fQBs[me_id]["ppr"] = fQBs[me_id]["ppr"] + [ppr]
            fQBs[me_id]["pprAct"] = fQBs[me_id]["pprAct"] + [qb["PPR"]]

            #
            # STD
            #
            std = (0.04 * predYPG) + (0.1 * predRushYPG) + (4.0 * predTD) + (6.0 * predRushTD) - (1.0 * predInt)

            fQBs[me_id]["std"] = fQBs[me_id]["std"] + [std]
            fQBs[me_id]["stdAct"] = fQBs[me_id]["stdAct"] + [qb["Fantasy Pts"]]

            #
            # Fan Duel / Yahoo / Half PPR
            #
            fd = (0.04 * predYPG) + (0.1 * predRushYPG) + (4.0 * predTD) + (6.0 * predRushTD) - (1.0 * predInt)

            fQBs[me_id]["fd"] = fQBs[me_id]["fd"] + [fd]
            fQBs[me_id]["fdAct"] = fQBs[me_id]["fdAct"] + [qb["FD Pts"]]

            # Test RMS Error
            if length > 9:
                error = dk - qb["DK Pts"]
                qbDKSqError = qbDKSqError + (error**2)
                qbDKCount += 1
                qbDKError = qbDKError + [error]
                qbDKPred = qbDKPred + [dk]
                qbDKAct = qbDKAct + [qb["DK Pts"]]

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

            # Save the most current Rush O Elo and Pass O Elo
            rushOelo = rushO[them_id]["elo"][-1]
            passOelo = passO[them_id]["elo"][-1]

            length = len(defense[me_id]["tacklesElo"])

        #### DEF: Tackles Elo
            # Calculate total tackles
            tackles = defn["Tackle Solo"] + 0.5*defn["Tackle Assist"]

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

        #### DEF: Tackle for Loss Elo
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

            rushDeloTD = rushD[us_id]["eloTD"][-1]
            rushOeloTD = rushO[them_id]["eloTD"][-1]
            passDeloTD = passD[us_id]["eloTD"][-1]
            passOeloTD = passO[them_id]["eloTD"][-1]

        #### RushD/RushO
            ## Rushing Elo
            # Team performance based on yards per game
            # Calculate point differential - in this case yards
            ptdiff = abs(team["Rush Yard opp"] - rushDdrawline)

            # Win if held offense to less than drawline
            win = team["Rush Yard opp"] < rushDdrawline

            # Compute new Elos
            dElo, oElo = eu.updateElo(rushDelo, rushOelo, ptdiff, win, blowoutFactor, teamK, both=True)

            # Update values
            rushD[us_id]["elo"] = rushD[us_id]["elo"] + [dElo]
            rushO[them_id]["elo"] = rushO[them_id]["elo"] + [oElo]

            rushD[us_id]["last"] = dElo
            rushO[them_id]["last"] = oElo

            ## Rushing EloTD
            # Calculate point differential - in this case yards
            ptdiff = abs(team["Rush TD opp"] - rushDTDline)

            # Win if held offense to less than drawline
            win = team["Rush TD opp"] < rushDTDline

            # Compute new Elos
            dEloTD, oEloTD = eu.updateElo(rushDeloTD, rushOeloTD, ptdiff, win, blowoutFactor, teamK, both=True)

            # Update values
            rushD[us_id]["eloTD"] = rushD[us_id]["eloTD"] + [dEloTD]
            rushO[them_id]["eloTD"] = rushO[them_id]["eloTD"] + [oEloTD]

            rushD[us_id]["lastTD"] = dEloTD
            rushO[them_id]["lastTD"] = oEloTD

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

        #### PassD/PassO
            ## Passing Elo
            # Team performance based on yards per game
            # Calculate point differential - in this case yards
            ptdiff = abs(team["Pass Yard opp"] - passDdrawline)

            # Win if held offense to less than drawline
            win = team["Pass Yard opp"] < passDdrawline

            # Compute new Elos
            dElo, oElo = eu.updateElo(passDelo, passOelo, ptdiff, win, blowoutFactor, teamK, both=True)

            # Update values
            passD[us_id]["elo"] = passD[us_id]["elo"] + [dElo]
            passO[them_id]["elo"] = passO[them_id]["elo"] + [oElo]

            passD[us_id]["last"] = dElo
            passO[them_id]["last"] = oElo

            ## Passing EloTD
            # Calculate point differential - in this case yards
            ptdiff = abs(team["Pass TD opp"] - passDTDline)

            # Win if held offense to less than drawline
            win = team["Pass TD opp"] < passDTDline

            # Compute new Elos
            dEloTD, oEloTD = eu.updateElo(passDeloTD, passOeloTD, ptdiff, win, blowoutFactor, teamK, both=True)

            # Update values
            passD[us_id]["eloTD"] = passD[us_id]["eloTD"] + [dEloTD]
            passO[them_id]["eloTD"] = passO[them_id]["eloTD"] + [oEloTD]

            passD[us_id]["lastTD"] = dEloTD
            passO[them_id]["lastTD"] = oEloTD

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

# Make into dataframes
rushD = pd.DataFrame.from_dict(rushD, orient="index").reset_index(drop=True)
rushO = pd.DataFrame.from_dict(rushO, orient="index").reset_index(drop=True)
passD = pd.DataFrame.from_dict(passD, orient="index").reset_index(drop=True)
passO = pd.DataFrame.from_dict(passO, orient="index").reset_index(drop=True)

# Calculate and print errors
if processRBs:
    fRBs = pd.DataFrame.from_dict(fRBs, orient="index").reset_index(drop=True)
    rbRMSE = sqrt(rbSqError / rbCount)
    rbYFSRMSE = sqrt(rbSqErrorYFS / rbCountYFS)
    rbRecRMSE = sqrt(rbRecSqError / rbRecCount)
    rbTDRMSE = sqrt(rbTDSqError / rbTDCount)
    rbDKRMSE = sqrt(rbDKSqError / rbDKCount)
    print("RBs")
    print(rbRMSE)
    print(rbYFSRMSE)
    print(rbRecRMSE)
    print(rbTDRMSE)
    print(rbDKRMSE)

if processWRs:
    fWRs = pd.DataFrame.from_dict(fWRs, orient="index").reset_index(drop=True)
    wrYpgRMSE = sqrt(wrYpgSqError / wrYpgCount)
    wrRecRMSE = sqrt(wrRecSqError / wrRecCount)
    wrTDRMSE = sqrt(wrTDSqError / wrTDCount)
    wrDKRMSE = sqrt(wrDKSqError / wrDKCount)
    print("WRs")
    print(wrYpgRMSE)
    print(wrRecRMSE)
    print(wrTDRMSE)
    print(wrDKRMSE)

if processTEs:
    fTEs = pd.DataFrame.from_dict(fTEs, orient="index").reset_index(drop=True)
    teYpgRMSE = sqrt(teYpgSqError / teYpgCount)
    teRecRMSE = sqrt(teRecSqError / teRecCount)
    teTDRMSE = sqrt(teTDSqError / teTDCount)
    teDKRMSE = sqrt(teDKSqError / teDKCount)
    print("TEs")
    print(teYpgRMSE)
    print(teRecRMSE)
    print(teTDRMSE)
    print(teDKRMSE)

if processQBs:
    fQBs = pd.DataFrame.from_dict(fQBs, orient="index").reset_index(drop=True)
    qbYpgRMSE = sqrt(qbYpgSqError / qbYpgCount)
    qbRushYpgRMSE = sqrt(qbRushYpgSqError / qbRushYpgCount)
    qbTDRMSE = sqrt(qbTDSqError / qbTDCount)
    qbRushTDRMSE = sqrt(qbRushTDSqError / qbRushTDCount)
    qbIntRMSE = sqrt(qbIntSqError / qbIntCount)
    qbDKRMSE = sqrt(qbDKSqError / qbDKCount)
    print("QBs")
    print(qbYpgRMSE)
    print(qbRushYpgRMSE)
    print(qbTDRMSE)
    print(qbRushTDRMSE)
    print(qbIntRMSE)
    print(qbDKRMSE)

if processDefense:
    defense = pd.DataFrame.from_dict(defense, orient="index").reset_index(drop=True)