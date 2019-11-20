# PURPOSE: Read in the team csv and get the CFB Power Five teams as an array to be used in further processing.
import pandas as pd
from pathlib import Path

# Path holding team data
root = Path("data_team")
p5file = root.joinpath("teams_ncaa.csv")

# Read in the team file and get Power Five teams
indata = pd.read_csv(p5file)

p5 = indata[indata["Power5"] == True]

power5teams = list(p5["TeamCode"])
