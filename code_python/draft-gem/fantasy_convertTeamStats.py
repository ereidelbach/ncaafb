# PURPOSE: Convert data from stats to games
import json
import pandas as pd
from pathlib import Path

from eloUtilities import get_game_code

# Seasons to run
seasons = range(1994, 2019 + 1)

# Root dir and names to join for reading/writing
elo_root = Path("..", "data_raw", "fantasy")
# Path to fantasy scraped data
old_root = Path("..", "data_scraped", "nfl_team")

# output file format (used in computeElo)
teamstatroot = "fantasy-team-game-statistics"
# input file format
teamroot = "nfl_team_data_"
extension = ".csv"

# Read in configfile
with open(Path("elo_config.json")) as file:
    cfg = json.load(file)["convert_team_stats"]

config = cfg["fantasy"]

from tqdm import tqdm

tqdm.write("Beginning TeamStats Conversion")

seasons = tqdm(seasons)
for season in seasons:
    seasons.set_description(f"s: {season}")

    infile = old_root.joinpath(f"{teamroot}{season}{extension}")
    outfile = elo_root.joinpath(f"{teamstatroot}{season}{extension}")

    teamall = pd.read_csv(infile)

    # Remove games past 16
    teamall = teamall[teamall["G"] <= 16]

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
