# PURPOSE: Write processed data to files for UI use
import pandas as pd
from pathlib import Path

from eloUtilities import save_elo_output
from cfb_computeElo import (
    rushers,
    receivers,
    passers,
    defense,
    rushD,
    rushO,
    passD,
    passO,
)

# Path to use for final data writing. Will create it if it doesn't already exist.
root = Path("data_raw")
if not root.exists():
    root.mkdir(parents=True)

# Acscending "To" and descending "last"
tl = [["last", "unique_id"], [False, True]]

# # # Potential restrictor query for rushers/receivers/passers/defense
# # # e.g., query=rc_0, query=rc_10, etc.
# rc_10 = "count > 10"

save_elo_output(rushers, root.joinpath("cfb_RB.csv"), tl[0], tl[1])
save_elo_output(receivers, root.joinpath("cfb_WR.csv"), tl[0], tl[1])
save_elo_output(passers, root.joinpath("cfb_QB.csv"), tl[0], tl[1])
save_elo_output(
    defense, root.joinpath("cfb_DEF.csv"), ["lastTackles", "unique_id"], tl[1]
)
save_elo_output(rushD, root.joinpath("team_cfb_rushDefense.csv"), ["last"], [False])
save_elo_output(rushO, root.joinpath("team_cfb_rushOffense.csv"), ["last"], [False])
save_elo_output(passD, root.joinpath("team_cfb_passDefense.csv"), ["last"], [False])
save_elo_output(passO, root.joinpath("team_cfb_passOffense.csv"), ["last"], [False])
