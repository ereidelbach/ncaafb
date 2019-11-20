# PURPOSE: Write processed data to files for UI use
import pandas as pd
from pathlib import Path

from eloUtilities import save_elo_output
from fantasy_computeElo import (
    fRBs,
    fWRs,
    fTEs,
    fQBs,
    # defense,
    rushD,
    rushO,
    passD,
    passO,
)

# Path to use for final data writing. Will create it if it doesn't already exist.
root = Path("..", "data_elo_calculated")
if not root.exists():
    root.mkdir(parents=True)

# Acscending "To" and descending "last"
tl = [["lastYPG", "unique_id"], [False, True]]

# # Potential restrictor query for rushers/receivers/passers/defense
# # e.g., query=rc_0, query=rc_10, etc.
rc_0 = "count > 0"

frb = save_elo_output(
    fRBs, root.joinpath("fantasy_RB.csv"), tl[0], tl[1], rc_0, retdf=True
)
fwr = save_elo_output(
    fWRs, root.joinpath("fantasy_WR.csv"), tl[0], tl[1], rc_0, retdf=True
)
fte = save_elo_output(
    fTEs, root.joinpath("fantasy_TE.csv"), tl[0], tl[1], rc_0, retdf=True
)
fqb = save_elo_output(
    fQBs, root.joinpath("fantasy_QB.csv"), tl[0], tl[1], rc_0, retdf=True
)
# save_elo_output(
#     defense, root.joinpath("fantasyD.csv"), ["lastTackles", "unique_id"], tl[1], rc_0
# )
save_elo_output(rushD, root.joinpath("fantasyRushDefense.csv"), ["last"], [False])
save_elo_output(rushO, root.joinpath("fantasyRushOffense.csv"), ["last"], [False])
save_elo_output(passD, root.joinpath("fantasyPassDefense.csv"), ["last"], [False])
save_elo_output(passO, root.joinpath("fantasyPassOffense.csv"), ["last"], [False])

# SECTION: Make json files for fantasy visualization
# Read in the player file (version: 2019-08-12)
player_data = pd.read_csv(
    Path("..", "data_elo_calculated", "FantasyLines", "nflPlayers_pg.csv")
)
# Merge the player_data (specifically, the college_id and pro_id) together with Elo output, then jsonify
import json

for k, v in zip([frb, fwr, fte, fqb], ["RBs", "WRs", "TEs", "QBs"]):
    outfile = str(
        Path("..", "data_elo_calculated", "FantasyLines", f"combo_fantasy{v}.json")
    )
    outdata = (
        pd.merge(k, player_data, how="inner", left_on="unique_id", right_on="pro_id")
        .fillna("")
        .set_index("master_id", drop=False)
        .to_dict(orient="index")
    )
    with open(outfile, "w") as f:
        json.dump(outdata, f)
