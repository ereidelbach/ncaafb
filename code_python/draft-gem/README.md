This folder contains python codes for scraping, elo calculation, and data combination for use in the UI.

- `code_python` folder of draft-analytics-data
- `coding` folder of draft-gem-elo

# Draft Gem Elo

- [Draft Gem Elo](#draft-gem-elo)
  - [Overview](#overview)
  - [Instructions](#instructions)
  - [Notes](#notes)
- [College Football Worklow](#college-football-worklow)
  - [NFL Workflow](#nfl-workflow)
      - [Overwriting median values for nfl](#overwriting-median-values-for-nfl)

## Overview

- code_python : holds all codes needed to compute Elo ratings. See below for more details.
- data_raw (../data_raw): has a folder each for CFB, NFL, and Fantasy leagues; these hold the files used in `compute_elo.py`

The file `elo_config.json` is a catch-all configuration file. It contains nested dictionaries that point to the files needed for each code, columns kept while processing data, Power Five teams, configuration information, etc. Multiple files read in this configuration file as "config" or "cfg" to access variables.

## Instructions

Running `cfb_saveOutput.py`, `nfl_saveOutput.py` or `fantasy_saveOutput.py` will import the needed objects and data from file to file.

To get drawlines for the NFL data, first run `nfl_findStats.py` to output pickle files. Running `python3 nfl_findStats.py` will show instructions.

Example CLI usage (NFL):

```
// navigate to the code folder
resirkin:projects$ cd draft-analytics-api/code_python

// if doing NFL, run `nflFindStats.py` with the `write` and `all` arguments to create pickle files
resirkin:projects/draft-analytics-api/code_python$ python3 nflFindStats.py all write

// OR, if doing NFL with updated numbers (combines seasons except 2019, similar to `nflPlayerGameStats`), run `nflFindStats.py` with the `write`, `all`, and `new` arguments to create pickle files
resirkin:projects/draft-analytics-api/code_python$ python3 nflFindStats.py all write new

// After one of the two options above, run `nflSaveOutput.py` to run Elo calculations and save data
resirkin:projects/draft-analytics-api/code_python$ python3 nflSaveOutput.py
```

## Notes

Change drawlines by altering variables in the `configuration` section near the top of the needed `computeElo` code.

If only running certain "positions" (e.g. `processRBs`, etc.), be sure to comment out the not-run positions at the top of `saveOutput` where the objects are imported. See below:

```
# Not running defense...
from eloUtilities import save_elo_output
from cfb_computeElo import (
    rushers,
    receivers,
    passers,
    ###defense,
    rushD,
    rushO,
    passD,
    passO,
)
```
# College Football Worklow
The steps below show the general workflow from starting with scraped data to computing and saving `csv`s of Elo output for COLLEGE (i.e. NCAA) data.

1. Run `cfb_runOldSeasons` (use team csv stats to make/initialize team Elo dataframes)

2. Run `cfb_computeElo` (use csvs from `1` and initialized dataframes from `2` to update team dataframes and make player position dataframes (e.g., rushers, receivers, passers, defense))
3. Run `cfb_saveOutput` (takes dataframes from `3` and writes csvs for UI)

If iterating through potential drawline differences:
Run `cfb_findStats` (calculates medians for drawlines)

## NFL Workflow
The steps below show the general workflow from starting with scraped data to computing and saving `csv`s of Elo output for NFL data.

1. Run `nfl_runOldSeasons` (use team csv stats to make/initialize team Elo dataframes)
   - Automatically runs `nfl_convertPlayerGameStats` & `nfl_ConvertTeamStats` which output edited csvs as part of the "running old seasons" process
2. Run `nfl_findStats write all` (calculates medians for drawlines) and saves them for compute elo
3. Run `nfl_computeElo` (use csvs from `1`, initialized dataframes from `2` to update team dataframes and make player position dataframes (e.g., rushers, receivers, passers, defense), and medians from `3` if not specifically over-ridden in this file)
4. Run `nfl_saveOutput` (takes dataframes from `4` and writes csvs for metadata mergin and UI)

#### Overwriting median values for nfl

- To overwrite a calculated median value: - Go to nflComputeElo - At the bottom of the file change where a value is loaded in from [qb/wr/te/rb]\_stats to the value wanted
