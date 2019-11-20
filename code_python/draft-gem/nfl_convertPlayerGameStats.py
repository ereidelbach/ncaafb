# PURPOSE: Convert data from stats to games
import json
import pandas as pd
from pathlib import Path

from eloUtilities import get_game_code, float_conv

# Seasons to run
seasons = range(1999, 2019 + 1)

# Root dir and names to join for reading/writing
root = Path("data_raw", "nfl")
# Path to nfl scraped data
old_root = Path("data_scraped", "nfl_player")

# output file format (used in computeElo)
playerstatroot = "nfl-player-game-statistics"
# input file format
playerroot = "nfl_player_"
extension = ".csv"

# Read in configfile
with open(Path("code_python/elo_config.json")) as file:
    cfg = json.load(file)["convert_player_game_stats"]

config = cfg["nfl"]

from tqdm import tqdm

tqdm.write("Beginning PlayerGameStats Conversion")
seasons = tqdm(seasons)
for season in seasons:
    seasons.set_description(f"s: {season}")

    infile = old_root.joinpath(f"{playerroot}{season}{extension}")
    outfile = root.joinpath(f"{playerstatroot}{season}{extension}")

    playerstats = pd.read_csv(infile, converters={"Catch Pct": float_conv})

    # Remove players without a unique_id (should be rare)
    playerall = playerstats.query("unique_id != 'none'").dropna(subset=["unique_id"])

    # Strip spaces from unique_id
    playerall["unique_id"] = playerall["unique_id"].str.strip()

    # TODO: There are some player stat games that don't have the opponent connected to them (sometimes they do appear in the team stats). For now, since there's not programmatic confirmation, remove those games missing that data.
    playerall = playerall.dropna(subset=["Team Code opp"])
    playerall = playerstats.dropna(subset=["unique_id"])

    # Also need to cast to int (because even though the zeroes are accounted for, the rest remain floats)
    playerall["Team Code"] = playerall["Team Code"].apply(lambda x: int(float(x)))
    playerall["Team Code opp"] = playerall["Team Code opp"].apply(
        lambda x: int(float(x))
    )

    # Do the game code calculation
    playerall["Game Code"] = playerall.apply(
        lambda x: get_game_code(
            x["Loc"], x["Team Code"], x["Team Code opp"], x["Date"]
        ),
        axis=1,
    )

    # Select columns & fill na values
    playerout = (
        playerall[config["keepcols"]]
        .fillna("0")
        .sort_values(["Game Code", "unique_id"])
        .reset_index(drop=True)
    )

    # Write final output to csv
    playerout.to_csv(outfile, index=False)
