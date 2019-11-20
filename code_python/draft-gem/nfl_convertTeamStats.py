# PURPOSE: Convert data from stats to games
import json
import pandas as pd
from pathlib import Path

from eloUtilities import get_game_code

# Seasons to run
seasons = range(1994, 2019 + 1)

# Root dir and name portions to join for reading/writing
elo_root = Path("data_raw", "nfl")
# Path to nfl scraped data
old_root = Path("data_scraped", "nfl_team")

# output file format (used in computeElo)
teamstatroot = "nfl-team-game-statistics"
# input file format
teamroot = "nfl_team_data_"
extension = ".csv"

# Read in configfile
with open(Path("code_python/elo_config.json")) as file:
    cfg = json.load(file)["convert_team_stats"]

config = cfg["nfl"]

from tqdm import tqdm

tqdm.write("Beginning TeamStats Conversion")
seasons = tqdm(seasons)
for season in seasons:
    seasons.set_description(f"s: {season}")

    teamfile = old_root.joinpath(f"{teamroot}{season}{extension}")
    outfile = elo_root.joinpath(f"{teamstatroot}{season}{extension}")

    teamall = pd.read_csv(teamfile)

    # Do the game code calculation
    teamall["Game Code"] = teamall.apply(
        lambda x: get_game_code(
            x["Loc"], x["Team Code"], x["Team Code opp"], x["Date"]
        ),
        axis=1,
    )

    # Select columns & fill na values
    teamout = (
        teamall[config["keepcols"]]
        .fillna("0")
        .sort_values(["Game Code", "Team Code"])
        .reset_index(drop=True)
    )

    # Write final output to csv
    teamout.to_csv(outfile, index=False)
