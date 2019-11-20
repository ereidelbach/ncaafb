#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# PURPOSE: Capture operations for processing Elo

# If you want to override any of the medians, simply do so at the bottom of this script

import json
import pandas as pd
import pickle
import numpy as np
from copy import deepcopy
from math import log, log10
from pathlib import Path


def EloWithDrawlines(
        initialPlayerElo,
        initialTeamElo,
        playerK,
        teamK,

        rushDdrawline,
        rushPdrawline,
        yfsPdrawline,
        passDdrawline,
        qbrDrawline,
        WRrecPdrawline,
        TErecPdrawline,
        RBTDline,
        WRTDline,
        TETDline,
        ydPerCarry,
        yardFactor,
        ypcFactor,
        yptFactor,
        fumline,

        passdrawline,
        WRtargetDrawline,
        TEtargetDrawline,
        compPercDrawline,
        passTDline,
        passIntline,
        WRrecDrawline,
        TErecDrawline,
        ydPerAttemptDrawline,
        QBydPerCatchDrawline,
        WRydPerCatchDrawline,
        TEydPerCatchDrawline,
        WRydPerTargetDrawline,
        TEydPerTargetDrawline,
        passYardFactor,
        passYPCFactor,
        pctFactor,
        recFactor,

        rushTeamDrawline,
        passTeamDrawline,
        tackleDrawline,
        tflDrawline,
        sackDrawline,
        qbHitsDrawline,
        intDrawline,
        ffDrawline,
        pbuDrawline,
        tdFactor,
        intFactor,
        blowoutFactor = True
        ):
    import eloUtilities as eu
    from nfl_runOldSeasons import rushD, rushO, passD, passO

    with open(Path("code_python/elo_config.json")) as file:
        cfg = json.load(file)["computeElo"]["nfl"]

    # Groups to process
    processRBs = True
    processWRs = True
    processTEs = True
    processRECIEVERs = True
    processQBs = True
    processDefense = True

    blowoutFactor = True

    seasons = range(1999, 2019 + 1)

    # Starter values
    initialEloList = [initialPlayerElo]
    initialDateList = [0]
    initialOppList = [0]

    # Holding and default dictionaries for each type of players
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

    if processTEs:
        tight_ends = {}
        # Initialize Receiving Elo data
        tight_ends_default = {
                "wRec": 0,
                "lRec": 0,
                "wYPG": 0,
                "lYPG": 0,
                "wYPC": 0,
                "lYPC": 0,
                "wTgt": 0,
                "lTgt": 0,
                "wYPT": 0,
                "lYPT": 0,
                "wTD": 0,
                "lTD": 0,
                "last": initialPlayerElo,
                "lastRec": initialPlayerElo,
                "lastYPG": initialPlayerElo,
                "lastYPC": initialPlayerElo,
                "lastTgt": initialPlayerElo,
                "lastYPT": initialPlayerElo,
                "lastTD": initialPlayerElo,
                "count": 0,
                "recElo": initialEloList,
                "ypgElo": initialEloList,
                "ypcElo": initialEloList,
                "tgtElo": initialEloList,
                "yptElo": initialEloList,
                "tdElo": initialEloList,
                "eloC": initialEloList,
                "date": initialDateList,
                "opp": initialOppList,
                }

    if processWRs:
        wide_receivers  = {}
        # Initialize Receiving Elo data
        wide_receivers_default = {
                "wRec": 0,
                "lRec": 0,
                "wYPG": 0,
                "lYPG": 0,
                "wYPC": 0,
                "lYPC": 0,
                "wTgt": 0,
                "lTgt": 0,
                "wYPT": 0,
                "lYPT": 0,
                "wTD": 0,
                "lTD": 0,
                "last": initialPlayerElo,
                "lastRec": initialPlayerElo,
                "lastYPG": initialPlayerElo,
                "lastYPC": initialPlayerElo,
                "lastTgt": initialPlayerElo,
                "lastYPT": initialPlayerElo,
                "lastTD": initialPlayerElo,
                "count": 0,
                "recElo": initialEloList,
                "ypgElo": initialEloList,
                "ypcElo": initialEloList,
                "tgtElo": initialEloList,
                "yptElo": initialEloList,
                "tdElo": initialEloList,
                "eloC": initialEloList,
                "date": initialDateList,
                "opp": initialOppList,
                }

    if processQBs:
        passers = {}
        # Initialize Passing Elo data
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
                "wQBH": 0,
                "lQBH": 0,
                "wINT": 0,
                "lINT": 0,
                "wPBU": 0,
                "lPBU": 0,
                "wFF": 0,
                "lFF": 0,
                "lastTackles": initialPlayerElo,
                "lastTFL": initialPlayerElo,
                "lastSack": initialPlayerElo,
                "lastQBH": initialPlayerElo,
                "lastInt": initialPlayerElo,
                "lastPBU": initialPlayerElo,
                "lastFF": initialPlayerElo,
                "count": 0,
                "tacklesElo": initialEloList,
                "tflElo": initialEloList,
                "sackElo": initialEloList,
                "qbhElo": initialEloList,
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
    demos = ["Pos"]

    extension = ".csv"

    playerstatroot = "nfl-player-game-statistics"
    teamstatroot = "nfl-team-game-statistics"

    # Do all the work - looping over each season
    from tqdm import tqdm
    tqdm.write("Beginning ComputeElo")
    seasons = tqdm(seasons)
    for season in seasons:
        seasons.set_description(f"s: {season}")
        # Read in files for the current season
        playerstatfile = Path("data_raw", "nfl", f"{playerstatroot}{season}{extension}")
        teamstatfile = Path("data_raw", "nfl", f"{teamstatroot}{season}{extension}")

        teamstats = eu.readteamgamedata(teamstatfile, nfl=True)

        # Read in and merge player stats with demographics/position
        playerstats = eu.readplayergamestats(playerstatfile)

        playerstats['Std Pos'] = playerstats['Pos'].str.strip()
        position_dict = { 'OLB' : 'LB',
                'ILB' : 'LB',
                'LB' : 'LB',
                'LILB' : 'LB',
                'RILB' : 'LB',
                'MLB' : 'LB',
                'LLB' : 'LB',
                'RLB' : 'LB',
                'SLB' : 'LB',
                'SAM' : 'LB',
                'LOLB' : 'LB',
                'ROLB' : 'LB',
                'WILL' : 'LB',
                'MIKE' : 'LB',
                'BLB' : 'LB',
                'CB' : 'DB',
                'DB' : 'DB',
                'RCB' : 'DB',
                'LCB' : 'DB',
                'S' : 'DB',
                'SS' : 'DB',
                'FS' : 'DB',
                'DE' : 'DL',
                'DL' : 'DL',
                'LDE' : 'DL',
                'RDE' : 'DL',
                'QB' : 'QB',
                'DT' : 'DL',
                'RDT' : 'DL',
                'LDT' : 'DL',
                'NT' : 'DL',
                'WDE' : 'DL',
                'NG' : 'DL',
                'SDE' : 'DL',
                'SB' : 'RB',
                'RB' : 'RB',
                'HB' : 'RB',
                'TB' : 'RB',
                'FB' : 'RB',
                'WR' : 'WR',
                'SE' : 'WR',
                'TE' : 'TE',
                'G' : 'OL',
                'OL' : 'OL',
                'C' : 'OL',
                'T' : 'OL',
                'OT' : 'OL',
                'OG' : 'OL',
                'LS' : 'OL'
                }
        playerstats['Std Pos'].replace(position_dict,inplace=True)

        dates = [int(x) for x in sorted(list(playerstats["gamedate"].unique()))]

        # Filter based on stats
        # FUTURE: change here to process based on player's position.
        # FUTURE: [player["position"].rsplit("/")] -- returns a list of positions the player has.
        if processRBs:
            # RBs - Filter to only players with at least 1 Rush Attempt
#            rbstats = playerstats[(playerstats["Std Pos"] == 'RB') & (playerstats["Rush Att"] > 0)].copy()
            rbstats = playerstats[(playerstats["Std Pos"].str.contains('RB')) & (playerstats["Rush Att"] > 0)].copy()

        if processTEs:
            # WRs/TEs - Filter to only players with at least 1 Catch
#            testats = playerstats[(playerstats["Std Pos"] == 'TE') & (playerstats["Targets"] > 0)].copy()
            testats = playerstats[(playerstats["Std Pos"].str.contains('TE')) & (playerstats["Targets"] > 0)].copy()

        if processWRs:
            # WRs/TEs - Filter to only players with at least 1 Catch
#            wrstats = playerstats[(playerstats["Std Pos"] == 'WR') & (playerstats["Targets"] > 0)].copy()
            wrstats = playerstats[(playerstats["Std Pos"].str.contains('WR')) & (playerstats["Targets"] > 0)].copy()

        if processQBs:
            # QBs - Filter to only players with at least 1 Pass Attempt
#            qbstats = playerstats[(playerstats["Std Pos"] == 'QB') & (playerstats["Pass Att"] > 0)].copy()
            qbstats = playerstats[(playerstats["Std Pos"].str.contains('QB')) & (playerstats["Pass Att"] > 0)].copy()

        if processDefense:
            # Defense - Filter to only players with some kind of tackle
#            defstats = playerstats[
#                    (playerstats["Std Pos"].isin(['DL','DB','LB'])) &
#                        ((playerstats["Tackle Solo"] > 0) | (playerstats["Tackle Assist"] > 0))
#                        ].copy()
            defstats = playerstats[
                    (playerstats["Std Pos"].str.contains('DL|LB|DB')) &
                        ((playerstats["Tackle Solo"] > 0) | (playerstats["Tackle Assist"] > 0) | (playerstats["Pass Broken Up"] > 0))
                        ].copy()

        #### Date in Dates
        # Loop over each date within the season - want to update Elo after each game
        for date in dates:
            # NOTE: Not expecting new NFL teams to crop up, but if they do or there's some other error with team code, this will cover them.
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
            if processTEs:
                tes = testats[testats["gamedate"] == date]
                te_teams = set(tes["awayteam"]).union(set(tes["hometeam"]))
                tes = tes.to_dict("records")
            if processQBs:
                qbs = qbstats[qbstats["gamedate"] == date]
                qb_teams = set(qbs["awayteam"]).union(set(qbs["hometeam"]))
                qbs = qbs.to_dict("records")
            if processDefense:
                defs = defstats[defstats["gamedate"] == date]
                def_teams = set(defs["awayteam"]).union(set(defs["hometeam"]))
                defs = defs.to_dict("records")

            # Create needed game dictionaries for teams
            position_teams = rb_teams.union(wr_teams).union(qb_teams).union(def_teams).union(te_teams)

            teams = teamstats[teamstats["gamedate"] == date].copy()
            teams_to_check = (
                    set(teams["awayteam"]).union(set(teams["hometeam"])).union(position_teams)
                    )

            # Initialize needed dictionaries
            for k in teams_to_check:
                if k not in rushD:
                    rushD[k] = deepcopy(teams_default)
                    rushD[k]["Team Code"] = k
                if k not in rushO:
                    rushO[k] = deepcopy(teams_default)
                    rushO[k]["Team Code"] = k
                if k not in passD:
                    passD[k] = deepcopy(teams_default)
                    passD[k]["Team Code"] = k
                if k not in passO:
                    passO[k] = deepcopy(teams_default)
                    passO[k]["Team Code"] = k

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
                    # Compute the point difference of win
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
                    td = rb["Rush TD"] + rb["Rec TD"]  # + rb$Kickoff.Ret.TD + rb$Punt.Ret.TD
                    # Compute point diff
                    ptdiff = abs(td - RBTDline) * 10
                    # Win if exceeds drawline
                    win = td > RBTDline
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
                    if me_id not in wide_receivers:
                        wide_receivers[me_id] = deepcopy(wide_receivers_default)
                        wide_receivers[me_id]["unique_id"] = me_id
                        # Add the demographic/position variables
                        for demo in demos:
                            wide_receivers[me_id][demo] = wr[demo]

                    # Save the most current Pass D ELO
                    passDelo = passD[them_id]["elo"][-1]

                    #### WR/TE: Receptions Per Game Elo
                    # Calculate point differential - in this case yards
                    ptdiff = abs(wr["Rec"] - WRrecDrawline) * 5
                    # Win if exceeded the drawline
                    win = wr["Rec"] > WRrecDrawline
                    # Grab current YPG Elo
                    recElo = wide_receivers[me_id]["recElo"][-1]
                    # Compute new Elo
                    Elo = eu.updateElo(recElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                    # Update values
                    wide_receivers[me_id]["recElo"] = wide_receivers[me_id]["recElo"] + [Elo]
                    wide_receivers[me_id]["lastRec"] = Elo
                    if win:
                        wide_receivers[me_id]["wRec"] += 1
                    else:
                        wide_receivers[me_id]["lRec"] += 1

                    #### WR/TE: Receiving Yards Per Game Elo
                    # Calculate point differential - in this case yards
                    ptdiff = abs(wr["Rec Yards"] - WRrecPdrawline) * yardFactor
                    # Win if exceeded the drawline
                    win = wr["Rec Yards"] > WRrecPdrawline
                    # Grab current YPG Elo
                    ypgElo = wide_receivers[me_id]["ypgElo"][-1]
                    # Compute new Elo
                    Elo = eu.updateElo(ypgElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                    # Update values
                    wide_receivers[me_id]["ypgElo"] = wide_receivers[me_id]["ypgElo"] + [Elo]
                    wide_receivers[me_id]["lastYPG"] = Elo
                    if win:
                        wide_receivers[me_id]["wYPG"] += 1
                    else:
                        wide_receivers[me_id]["lYPG"] += 1

                    #### WR/TE: Yards Per Catch Elo
                    # Compute adjusted yards per catch based on log10 of 2 times number of catches
                    ypc = 0
                    if wr["Rec"] > 0:
                        ypc = wr["Rec Yards"] / wr["Rec"]
                    # Compute the point difference of win
                    ptdiff = abs(ypc - WRydPerCatchDrawline) * ypcFactor
                    # Is the ypc a win?
                    win = ypc > WRydPerCatchDrawline
                    # Grab current YPC Elo
                    ypcElo = wide_receivers[me_id]["ypcElo"][-1]
                    # Compute new YPC Elo
                    Elo = eu.updateElo(ypcElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                    # Update values
                    wide_receivers[me_id]["ypcElo"] = wide_receivers[me_id]["ypcElo"] + [Elo]
                    wide_receivers[me_id]["lastYPC"] = Elo
                    if win:
                        wide_receivers[me_id]["wYPC"] += 1
                    else:
                        wide_receivers[me_id]["lYPC"] += 1

                    #### WR/TE: Target Percentage Elo
                    # Compute percentage of catches vs targets
                    tgt = wr["Rec"] / wr["Targets"] * 100
                    # Compute the point difference of win
                    ptdiff = abs(tgt - WRtargetDrawline)
                    # Is the ypc a win?
                    win = tgt > WRtargetDrawline
                    # Grab current YPC Elo
                    tgtElo = wide_receivers[me_id]["tgtElo"][-1]
                    # Compute new YPC Elo
                    Elo = eu.updateElo(tgtElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                    # Update values
                    wide_receivers[me_id]["tgtElo"] = wide_receivers[me_id]["tgtElo"] + [Elo]
                    wide_receivers[me_id]["lastTgt"] = Elo
                    if win:
                        wide_receivers[me_id]["wTgt"] += 1
                    else:
                        wide_receivers[me_id]["lTgt"] += 1

                    #### WR/TE:  Yards Per Target Elo
                    # Compute adjusted yards per catch based on log10 of the 2 times the number of catches
                    ypt = wr["Rec Yards"] / wr["Targets"]
                    # Compute the point difference of win
                    ptdiff = abs(ypt - WRydPerTargetDrawline) * yptFactor
                    # Is the ypc a win?
                    win = ypt > WRydPerTargetDrawline
                    # Grab current YPC Elo
                    yptElo = wide_receivers[me_id]["yptElo"][-1]
                    # Compute new YPC Elo
                    Elo = eu.updateElo(yptElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                    # Update values
                    wide_receivers[me_id]["yptElo"] = wide_receivers[me_id]["yptElo"] + [Elo]
                    wide_receivers[me_id]["lastYPT"] = Elo
                    if win:
                        wide_receivers[me_id]["wYPT"] += 1
                    else:
                        wide_receivers[me_id]["lYPT"] += 1

                    #### WR/TE: TD Elo
                    # Sum all TDs
                    td = wr["Rec TD"]  # + wr$Kickoff.Ret.TD + wr$Punt.Ret.TD
                    # Compute point diff
                    ptdiff = abs(td - WRTDline) * 10
                    # Win if exceeds drawline
                    win = td > WRTDline
                    # Grab current TD Elo
                    tdElo = wide_receivers[me_id]["tdElo"][-1]
                    # Compute new Elo
                    Elo = eu.updateElo(tdElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                    # Update Values
                    wide_receivers[me_id]["tdElo"] = wide_receivers[me_id]["tdElo"] + [Elo]
                    wide_receivers[me_id]["lastTD"] = Elo
                    if win:
                        wide_receivers[me_id]["wTD"] += 1
                    else:
                        wide_receivers[me_id]["lTD"] += 1

                    #### WR/TE: Dates and Cumulative Elo
                    # Accumulate Elo scores into a cumulative Elo
                    recElo = wide_receivers[me_id]["recElo"][-1]
                    ypgElo = wide_receivers[me_id]["ypgElo"][-1]
                    ypcElo = wide_receivers[me_id]["ypcElo"][-1]
                    tgtElo = wide_receivers[me_id]["tgtElo"][-1]
                    yptElo = wide_receivers[me_id]["yptElo"][-1]
                    tdElo = wide_receivers[me_id]["tdElo"][-1]

                    # Compute Composite Elo
                    value = round(
                            (recElo + ypgElo + (ypcElo + yptElo) / 2 + tgtElo + tdElo) / 5
                            )

                    # Update values
                    wide_receivers[me_id]["eloC"] = wide_receivers[me_id]["eloC"] + [value]
                    wide_receivers[me_id]["last"] = value

                    # Add the date (if first time, add a date for the first date)
                    if wide_receivers[me_id]["count"] == 0:
                        wide_receivers[me_id]["date"][0] = eu.get_start_date(date)
                    wide_receivers[me_id]["date"] = wide_receivers[me_id]["date"] + [date]

                    # Add the opponent
                    wide_receivers[me_id]["opp"] = wide_receivers[me_id]["opp"] + [wr["Team Code opp"]]

                    # Count # of games the player has data for
                    wide_receivers[me_id]["count"] += 1

            #### WR/TE Evaluation
            if processTEs and len(tes) > 0:
                for te in tes:
                    # Player's `unique_id` and opponent team's `Team Code`
                    me_id = te["unique_id"]
                    them_id = te["Team Code opp"]

                    # Initialize a player dictionary if not already present
                    if me_id not in tight_ends:
                        tight_ends[me_id] = deepcopy(tight_ends_default)
                        tight_ends[me_id]["unique_id"] = me_id
                        # Add the demographic/position variables
                        for demo in demos:
                            tight_ends[me_id][demo] = te[demo]

                    # Save the most current Pass D ELO
                    passDelo = passD[them_id]["elo"][-1]

                    #### WR/TE: Receptions Per Game Elo
                    # Calculate point differential - in this case yards
                    ptdiff = abs(te["Rec"] - TErecDrawline) * 5
                    # Win if exceeded the drawline
                    win = te["Rec"] > TErecDrawline
                    # Grab current YPG Elo
                    recElo = tight_ends[me_id]["recElo"][-1]
                    # Compute new Elo
                    Elo = eu.updateElo(recElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                    # Update values
                    tight_ends[me_id]["recElo"] = tight_ends[me_id]["recElo"] + [Elo]
                    tight_ends[me_id]["lastRec"] = Elo
                    if win:
                        tight_ends[me_id]["wRec"] += 1
                    else:
                        tight_ends[me_id]["lRec"] += 1

                    #### te/TE: Receiving Yards Per Game Elo
                    # Calculate point differential - in this case yards
                    ptdiff = abs(te["Rec Yards"] - TErecPdrawline) * yardFactor
                    # Win if exceeded the drawline
                    win = te["Rec Yards"] > TErecPdrawline
                    # Grab current YPG Elo
                    ypgElo = tight_ends[me_id]["ypgElo"][-1]
                    # Compute new Elo
                    Elo = eu.updateElo(ypgElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                    # Update values
                    tight_ends[me_id]["ypgElo"] = tight_ends[me_id]["ypgElo"] + [Elo]
                    tight_ends[me_id]["lastYPG"] = Elo
                    if win:
                        tight_ends[me_id]["wYPG"] += 1
                    else:
                        tight_ends[me_id]["lYPG"] += 1

                    #### WR/TE: Yards Per Catch Elo
                    # Compute adjusted yards per catch based on log10 of 2 times number of catches
                    ypc = 0
                    if te["Rec"] > 0:
                        ypc = te["Rec Yards"] / te["Rec"]
                    # Compute the point difference of win
                    ptdiff = abs(ypc - TEydPerCatchDrawline) * ypcFactor
                    # Is the ypc a win?
                    win = ypc > TEydPerCatchDrawline
                    # Grab current YPC Elo
                    ypcElo = tight_ends[me_id]["ypcElo"][-1]
                    # Compute new YPC Elo
                    Elo = eu.updateElo(ypcElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                    # Update values
                    tight_ends[me_id]["ypcElo"] = tight_ends[me_id]["ypcElo"] + [Elo]
                    tight_ends[me_id]["lastYPC"] = Elo
                    if win:
                        tight_ends[me_id]["wYPC"] += 1
                    else:
                        tight_ends[me_id]["lYPC"] += 1

                    #### WR/TE: Target Percentage Elo
                    # Compute percentage of catches vs targets
                    tgt = te["Rec"] / te["Targets"] * 100
                    # Compute the point difference of win
                    ptdiff = abs(tgt - TEtargetDrawline)
                    # Is the ypc a win?
                    win = tgt > TEtargetDrawline
                    # Grab current YPC Elo
                    tgtElo = tight_ends[me_id]["tgtElo"][-1]
                    # Compute new YPC Elo
                    Elo = eu.updateElo(tgtElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                    # Update values
                    tight_ends[me_id]["tgtElo"] = tight_ends[me_id]["tgtElo"] + [Elo]
                    tight_ends[me_id]["lastTgt"] = Elo
                    if win:
                        tight_ends[me_id]["wTgt"] += 1
                    else:
                        tight_ends[me_id]["lTgt"] += 1

                    #### WR/TE:  Yards Per Target Elo
                    # Compute adjusted yards per catch based on log10 of the 2 times the number of catches
                    ypt = te["Rec Yards"] / te["Targets"]
                    # Compute the point difference of win
                    ptdiff = abs(ypt - TEydPerTargetDrawline) * yptFactor
                    # Is the ypc a win?
                    win = ypt > TEydPerTargetDrawline
                    # Grab current YPC Elo
                    yptElo = tight_ends[me_id]["yptElo"][-1]
                    # Compute new YPC Elo
                    Elo = eu.updateElo(yptElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                    # Update values
                    tight_ends[me_id]["yptElo"] = tight_ends[me_id]["yptElo"] + [Elo]
                    tight_ends[me_id]["lastYPT"] = Elo
                    if win:
                        tight_ends[me_id]["wYPT"] += 1
                    else:
                        tight_ends[me_id]["lYPT"] += 1

                    #### WR/TE: TD Elo
                    # Sum all TDs
                    td = te["Rec TD"]  # + te$Kickoff.Ret.TD + te$Punt.Ret.TD
                    # Compute point diff
                    ptdiff = abs(td - TETDline) * 10
                    # Win if exceeds drawline
                    win = td > TETDline
                    # Grab current TD Elo
                    tdElo = tight_ends[me_id]["tdElo"][-1]
                    # Compute new Elo
                    Elo = eu.updateElo(tdElo, passDelo, ptdiff, win, blowoutFactor, playerK)

                    # Update Values
                    tight_ends[me_id]["tdElo"] = tight_ends[me_id]["tdElo"] + [Elo]
                    tight_ends[me_id]["lastTD"] = Elo
                    if win:
                        tight_ends[me_id]["wTD"] += 1
                    else:
                        tight_ends[me_id]["lTD"] += 1

                    #### WR/TE: Dates and Cumulative Elo
                    # Accumulate Elo scores into a cumulative Elo
                    recElo = tight_ends[me_id]["recElo"][-1]
                    ypgElo = tight_ends[me_id]["ypgElo"][-1]
                    ypcElo = tight_ends[me_id]["ypcElo"][-1]
                    tgtElo = tight_ends[me_id]["tgtElo"][-1]
                    yptElo = tight_ends[me_id]["yptElo"][-1]
                    tdElo = tight_ends[me_id]["tdElo"][-1]

                    # Compute Composite Elo
                    value = round(
                            (recElo + ypgElo + (ypcElo + yptElo) / 2 + tgtElo + tdElo) / 5
                            )

                    # Update values
                    tight_ends[me_id]["eloC"] = tight_ends[me_id]["eloC"] + [value]
                    tight_ends[me_id]["last"] = value

                    # Add the date (if first time, add a date for the first date)
                    if tight_ends[me_id]["count"] == 0:
                        tight_ends[me_id]["date"][0] = eu.get_start_date(date)
                    tight_ends[me_id]["date"] = tight_ends[me_id]["date"] + [date]

                    # Add the opponent
                    tight_ends[me_id]["opp"] = tight_ends[me_id]["opp"] + [te["Team Code opp"]]

                    # Count # of games the player has data for
                    tight_ends[me_id]["count"] += 1

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

                    # Save the most current Pass D Elo
                    passDelo = passD[them_id]["elo"][-1]

                    #### QBs: QBR Elo
                    # Compute QBR - From https://en.wikipedia.org/wiki/Passer_rating using NCAA rating formula
                    # qbr = ((8.4 * qb["Pass.Yard) + (330 * qb["Pass.TD) + (100 * qb["Pass.Comp) - (200 * qb["Pass.Int)) / qb["Pass.Att
                    qbr = qb["QBR"]
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
                    ptdiff = abs(ypc - QBydPerCatchDrawline) * passYPCFactor
                    # Win if exceeds drawline
                    win = ypc > QBydPerCatchDrawline
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

                    #### QBs: Completion Percentage Elo`
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
                    td = qb["Pass TD"]
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

                    #### QBs: Dates and Cumulative Elo
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
                        passers[me_id]["date"][0] = eu.get_start_date(date)  # date - 1
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

                    # Save the most current Rush O Elo and Pass O Elo
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

                    #### DEF: Tackle for Loss Elo
                    # Calculate TFL
                    tfl = defn["Tackle for Loss"]
                    # Calculate point differential - in this case tackles for loss
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
                    # Calculate point differential - in this case quarterback sacks
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

                    #### DEF: QB Hits Elo
                    # Calculate Sacks
                    qbh = defn["QBHits"]
                    # Calculate point differential - in this case quarteback hits
                    ptdiff = abs(qbh - qbHitsDrawline) * 10
                    # Win if exceeds drawline
                    win = qbh >= qbHitsDrawline
                    # Grab current TFL Elo
                    qbhElo = defense[me_id]["qbhElo"][-1]
                    # Compute new Elo
                    Elo = eu.updateElo(qbhElo, passOelo, ptdiff, win, blowoutFactor, playerK)

                    # Update values
                    defense[me_id]["qbhElo"] = defense[me_id]["qbhElo"] + [Elo]
                    defense[me_id]["lastQBH"] = Elo
                    if win:
                        defense[me_id]["wQBH"] += 1
                    else:
                        defense[me_id]["lQBH"] += 1

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
                    # Calculate point differential - in this case passes broken up
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
                    # Calculate Forced Fumble Elo
                    ff = defn["Fumble Forced"]
                    # Calculate point differential - in this case forced fumbles
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
                        defense[me_id]["date"][0] = eu.get_start_date(date)  # date - 1
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

                    # Update values
                    rushD[us_id]["elo"] = rushD[us_id]["elo"] + [dElo]
                    rushO[them_id]["elo"] = rushO[them_id]["elo"] + [oElo]

                    rushD[us_id]["last"] = dElo
                    rushO[them_id]["last"] = oElo

                    #### PassD/PassO
                    # Team performance based on yards per game
                    # Calculate point differential - in this case yards
                    ptdiff = abs(team["Pass Yard opp"] - passTeamDrawline) / passYardFactor

                    # Win if held offense to less than drawline
                    win = team["Pass Yard opp"] < passTeamDrawline

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
        wide_receivers = pd.DataFrame.from_dict(wide_receivers, orient="index").reset_index(drop=True)
    if processTEs:
        tight_ends = pd.DataFrame.from_dict(tight_ends, orient="index").reset_index(drop=True)
    if processQBs:
        passers = pd.DataFrame.from_dict(passers, orient="index").reset_index(drop=True)
    if processDefense:
        defense = pd.DataFrame.from_dict(defense, orient="index").reset_index(drop=True)

    return [rushers, wide_receivers, tight_ends, passers, defense, rushD, rushO, passD, passO]


try:
    qb_stats = pickle.load(open('data_raw/nfl/qb_stats.pickle','rb'))
    rb_stats = pickle.load(open('data_raw/nfl/rb_stats.pickle','rb'))
    wr_stats = pickle.load(open('data_raw/nfl/wr_stats.pickle','rb'))
    te_stats = pickle.load(open('data_raw/nfl/te_stats.pickle','rb'))
except FileNotFoundError as e:
    print(e)
    print("Saved median values not found")
    print('Run nflFindStats.py with "write" as argument before running nflComputeElo.py')
    exit()

# print(qb_stats)
# print(rb_stats)
# print(te_stats)
# print(wr_stats)

[rushers, wide_receivers, tight_ends, passers, defense, rushD, rushO, passD, passO] = EloWithDrawlines(
        initialPlayerElo = 1300,
        initialTeamElo = 1300,
        playerK = 20,
        teamK = 20,

        rushDdrawline = 110,
        rushPdrawline = 46,
        yfsPdrawline = rb_stats["yfsPdrawline"],
        passDdrawline = 157,
        qbrDrawline = qb_stats["qbrDrawline"],
        WRrecPdrawline = wr_stats["recPdrawline"],
        TErecPdrawline = te_stats["recPdrawline"],
        RBTDline = rb_stats["TDline"],
        WRTDline = wr_stats["TDline"],
        TETDline = te_stats["TDline"],
        ydPerCarry = rb_stats["ydPerCarry"],
        yardFactor = 1,
        ypcFactor = 5,
        yptFactor = 5,
        fumline = 0.1,

        passdrawline = 157,
        WRtargetDrawline = wr_stats["targetDrawline"],
        TEtargetDrawline = te_stats["targetDrawline"],
        compPercDrawline = qb_stats["compPercDrawline"] ,
        passTDline = qb_stats["passTDline"],
        passIntline = qb_stats["passIntline"],
        WRrecDrawline = wr_stats["recDrawline"],
        TErecDrawline = te_stats["recDrawline"],
        ydPerAttemptDrawline = qb_stats["ydPerAttemptDrawline"],
        QBydPerCatchDrawline = qb_stats["ydPerCatchDrawline"],
        WRydPerCatchDrawline = wr_stats["ydPerCatchDrawline"],
        TEydPerCatchDrawline = te_stats["ydPerCatchDrawline"],
        WRydPerTargetDrawline = wr_stats["ydPerTargetDrawline"],
        TEydPerTargetDrawline = te_stats["ydPerTargetDrawline"],
        passYardFactor = 5,
        passYPCFactor = 5,
        pctFactor = 2,
        recFactor = 3,

        rushTeamDrawline = 110,
        passTeamDrawline = 214,
        tackleDrawline = 2.0  ,
        tflDrawline = 0.3,
        sackDrawline = 0.05,
        qbHitsDrawline = 0.1,
        intDrawline = 0.05,
        ffDrawline = 0.05,
        pbuDrawline = 0.1,
        tdFactor = 5,
        intFactor = 5
        )
