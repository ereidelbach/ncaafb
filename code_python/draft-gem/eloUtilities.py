#!/usr/bin/env python3
import numpy as np
import pandas as pd

from ast import literal_eval
from copy import deepcopy
from datetime import datetime, timedelta
from math import log, log10
from pathlib import Path


def probability(rating1, rating2):
    """
    Converting Elos into probability.

    Input
    ----------
        rating1 : integer
            Elo of performer 1
        rating2 : integer
            Elo of performer 2 -- this is the probability returned
    Returns
    -------
        prob : float
            Probability of rating2 to "win"
    """
    prob = 1.0 / (1.0 + 10.0 ** ((rating1 - rating2) / 400))
    return prob


def elo(Ra, Rb, K, win):
    """
    Compute the new ELO score for Ra

    Input
    ----------
        Ra : integer
            Ra Elo
        Rb : integer
            Rb Elo
        K : number
            Elo K factor
        win : boolean
            True if Ra won
    Returns
    ----------
        Ra : integer
            new Elo for Ra
    """
    Pa = probability(Rb, Ra)
    if win:
        Ra = Ra + K * (1 - Pa)
    else:
        Ra = Ra + K * (0 - Pa)
    Ra = round(Ra)
    return Ra


def multiplier(ptdiff, elodiff):
    """
    Fivethirtyeight.com scale factor for big win
    """
    factor = log(ptdiff + 1) * (2.2 / (elodiff * 0.001 + 2.2))
    # factor = log(ptdiff + 1)
    return factor


def updateElo(myCurrentElo, oppCurrentElo, ptdiff, win, scale, K, both=False):
    """
    Update Elo calculation

    Input
    ----------
        myCurrentElo : integer
            Elo my team starts at
        oppCurrentElo : integer
            Elo opponent starts at
        ptdiff : number
            difference in points
        win : boolean
            whether my team won
        scale : boolean
            whether to scale win based on `ptdiff` (aka blowoutFactor)
        K : number
            K-factor
        both : boolean
            whether to also return the opponent's new Elo
    Returns
    ----------
        myNewElo : integer
            New Elo rating of my team
        oppNewElo : integer [if `both == True`]
            New Elo rating of opponent's team
    """
    marginmultiplier = 1
    # If we are scaling the win based on the point diff
    if scale:
        if win:
            elodiff = myCurrentElo - oppCurrentElo
        else:
            elodiff = oppCurrentElo - myCurrentElo

        # Margin multiplier formula taken from fivethirtyeight.com
        marginmultiplier = multiplier(ptdiff, elodiff)
    # Compute new Elo
    myNewElo = elo(myCurrentElo, oppCurrentElo, marginmultiplier * K, win)
    if both:
        oppNewElo = elo(oppCurrentElo, myCurrentElo, marginmultiplier * K, not win)
        return myNewElo, oppNewElo
    else:
        return myNewElo


def convertToInt(x):
    return int(float(x))


def float_conv(x):
    if x == "":
        return 0.0
    else:
        return float(x[:-1])


def readteamgamedata(filename, nfl=False):
    """
    Read in a season's worth of team game data from cfbstats
        `Format`: team-game-statistics.csv - from cfbstats

    Input
    ----------
        filename : string
            filename to read in
    Returns
    ----------
        team2 : dataframe
            sorted-by-date version of the season's games
    """
    print(filename)
    raw = pd.read_csv(
        filename,
        converters={
            "Rk": float,
            "Game Code": str,
            "A": str,
            "School": str,
            "Team Code": int,
        },
    )
#    testJoin = raw
#    if nfl:
#        testJoin = raw
#    else:
#        testJoin = pd.merge(raw, raw, on="Game Code", suffixes=("", " opp"))
#    team = testJoin[testJoin["Team Code"] != testJoin["Team Code opp"]].copy()
    team = raw[raw["Team Code"] != raw["Team Code opp"]].copy()

    team["awayteam"] = team["Game Code"].str[0:4].astype(int)
    team["hometeam"] = team["Game Code"].str[4:8].astype(int)
    team["gamedate"] = team["Game Code"].str[8:16].astype(int)

    team2 = team.sort_values("gamedate")
    return team2


def readplayergamestats(playerstatsfilename, limit_cols=None):
    """
    Read in a season's worth of player game data from cfbstats
        `Format`: player-game-statistics.csv - from cfbstats

    Input
    ----------
        filename : string
            filename to read in
    Returns
    ----------
        playerstat : dataframe
            sorted-by-date version of the season's games
    """
    playerstat = pd.read_csv(
        playerstatsfilename,
        converters=(
            {
                "Game Code": str,
                "Player Code": int,
                "Team Code": int,
                "Team Code opp": int,
            }
        ),
        usecols=limit_cols,
    )
    # playerstat = merge(raw, playerLookup, by="Player.Code")
    playerstat["awayteam"] = playerstat["Game Code"].str[0:4].astype(int)
    playerstat["hometeam"] = playerstat["Game Code"].str[4:8].astype(int)
    playerstat["gamedate"] = playerstat["Game Code"].str[8:16].astype(int)
    #    for row in 1:nrow(playerstat):
    #        if(playerstat$Team.Code[row] == playerstat$hometeam[row])
    #            playerstat$Team.Code.opp[row] = playerstat$awayteam[row]
    #        else
    #            playerstat$Team.Code.opp[row] = playerstat$hometeam[row]
    playerstat = playerstat.sort_values("gamedate")
    return playerstat


def calcQBR(passYards, passTD, passComp, passInt, passAtt):
    """
    Compute QBR - From https://en.wikipedia.org/wiki/Passer_rating using NCAA rating formula
    """
    qbr = (
        (8.4 * passYards) + (330 * passTD) + (100 * passComp) - (200 * passInt)
    ) / passAtt
    return qbr


def away_and_home(loc, team_code, opp_team_code):
    """
    Take in a loc, a team code, and an opponent code
    Return an 8-character string, the away and home codes with leading zeros as needed (e.g., "01230038" for team codes 123 and 38)
    """
    team_f = f"{team_code:04}"
    opp_f = f"{opp_team_code:04}"
    if loc == "@":
        away = team_f
        home = opp_f
    else:
        if loc == "N":
            if team_code > opp_team_code:
                away = team_f
                home = opp_f
            else:
                home = team_f
                away = opp_f
        else:
            home = team_f
            away = opp_f
    return f"{away}{home}"


def get_game_code(loc, team_code, opp_team_code, date):
    """
    Take in a loc, team code, opponent code, and date (usually as lambda apply on a df from a row of `playerall`)
    Return the string game code.
    """
    newDate = date.replace("-", "")
    awayhome = away_and_home(loc, team_code, opp_team_code)
    # nanoseconds different
    # return f"{awayhome}{newDate}"
    return awayhome + newDate


def get_start_date(date):
    """
    Input: date for a player (int)
    Return: initialized start date (aka date - 1)
    """
    # Convert to datetime and subtract a day
    newdate = datetime.strptime(str(date), "%Y%m%d") - timedelta(days=1)
    # Format just YYYYMMDD as string
    startdate = newdate.strftime("%Y%m%d")
    return int(startdate)


def save_elo_output(
    frame, outfile, arranger_cols, arranger_asc, query=None, retdf=False
):
    """
    dataframe to analyze
    output filepath
    how to sort the columns: lists must be same length
        list of strings for column names
        list of booleans for if column should be sorted ASCENDING or not
    [OPT]: pandas-style query string to restrict dataset
        (e.g., "count > 0")
    """
    # Argument check:
    if len(arranger_cols) != len(arranger_asc):
        raise ValueError("arranger_cols must be length of arranger_asc")

    # Arrange the df
    ordered = frame.sort_values(by=arranger_cols, ascending=arranger_asc)

    if query is not None:
        ordered = ordered.query(query)

    # Write the file
    ordered.to_csv(outfile, index=False)

    # Return it if needed
    if retdf:
        return ordered


def nested_list_column(n_lists, value):
    """
    Make a nested list containing `value` as first element, for easy insertion into df

    Input
    ----------
        n_lists : integer
            number of lists to make
        value : variable
            value to populate internal lists
    Returns
    ----------
        output : list
            nested list of length n_lists, each internal list as value
    """
    output = [[value] for k in range(n_lists)]
    return output


def listify_df_cols(df, columnlist):
    """
    Turn the specified columns into list versions of themselves
    (From a Python-written csv that has lists as internal elements)
    """
    if type(columnlist) is str:
        columnlist = [columnlist]
    for k in columnlist:
        df[k] = df[k].apply(lambda x: literal_eval(x))
    return df


def exact_compare_dfs(df_py, df_r, sorters=["unique_id", "G"]):
    rv = df_r.sort_values(sorters).reset_index(drop=True).sort_index(axis=1)
    pyv = df_py.sort_values(sorters).reset_index(drop=True).sort_index(axis=1)
    rv.columns = pyv.columns
    return rv.equals(pyv)


def sort_glob(path, pattern):
    """
    With a pathlib `path` object as input, return a sorted, globbed list matching `pattern`.
    """
    return sorted(list(path.glob(pattern)))


def collist(frame):
    """
    Get a sorted list of a dataframe"s columns.

    Parameters
    ----------
        frame : pd.DataFrame
    Returns
    -------
        Sorted list of df columns.
    """
    return sorted(list(frame.columns))


def dfsortcols(frame):
    """
    Return a dataframe with sorted columns
    """
    return frame.reindex(columns=collist(frame))


def get_needed_dict(
    unique_id, source_dict, default_dict, id_name="unique_id", indicator=False
):
    """
    Take in a unique_id for a nested dictionary in source_dict.
    If the unique_id does not exist, create a default_dict with the unique_id.
    Return either the found or default dictionary.
    ---
    If `indicator` is True, also returns whether the default was used.
    """
    default = False
    if unique_id in source_dict:
        out_dict = deepcopy(source_dict[unique_id])
    else:
        out_dict = deepcopy(default_dict)
        out_dict[id_name] = unique_id
        default = True
    if indicator:
        return out_dict, default
    else:
        return out_dict
