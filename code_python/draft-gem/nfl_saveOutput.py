# PURPOSE: Write processed data to files for UI use
import pandas as pd
from pathlib import Path

from eloUtilities import save_elo_output
from nfl_computeElo import (
    rushers,
    wide_receivers,
    tight_ends,
    passers,
    defense,
    rushD,
    rushO,
    passD,
    passO,
)

receivers = wide_receivers.append(tight_ends, ignore_index=True)


# Path to use for final data writing. Will create it if it doesn't already exist.
root = Path("data_raw")
if not root.exists():
    root.mkdir(parents=True)

# Acscending "unique_id" and descending "last"
tl = [["last", "unique_id"], [False, True]]

# # Potential restrictor query for rushers/receivers/passers/defense
# # e.g., query=rc_0, query=rc_10, etc.
rc_0 = "count > 0"

save_elo_output(rushers, root.joinpath("nfl_RB.csv"), tl[0], tl[1], rc_0)
save_elo_output(receivers, root.joinpath("nfl_WR.csv"), tl[0], tl[1], rc_0)
save_elo_output(passers, root.joinpath("nfl_QB.csv"), tl[0], tl[1], rc_0)
save_elo_output(
    defense, root.joinpath("nfl_DEF.csv"), ["lastTackles", "unique_id"], tl[1], rc_0
)
save_elo_output(rushD, root.joinpath("team_nfl_RushDefense.csv"), ["last"], [False])
save_elo_output(rushO, root.joinpath("team_nfl_RushOffense.csv"), ["last"], [False])
save_elo_output(passD, root.joinpath("team_nfl_PassDefense.csv"), ["last"], [False])
save_elo_output(passO, root.joinpath("team_nfl_PassOffense.csv"), ["last"], [False])
