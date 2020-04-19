"""
Compare ELO-based model to masterpoints model, for predicting the result of
a match-up of two players.
"""
import sqlite3
import numpy as np
from sklearn.metrics import accuracy_score

"""
Load a table of ELO scores and masterpoints for each player.
Shape (ntest, 3)
(acbl_number, masterpoints, elo)
"""

# Map each single player to their masterpoint total.
acbl_num_to_masterpoints = {}
conn = sqlite3.connect("../data/bridge.db")
res = conn.execute("""
    select player1_acbl_number, player1_masterpoints,
           player2_acbl_number, player2_masterpoints
    from pairs
        """)
for (player1_acbl_number, player1_masterpoints, 
    player2_acbl_number, player2_masterpoints) in res:
    
        acbl_num_to_masterpoints[player1_acbl_number] = player1_masterpoints
        acbl_num_to_masterpoints[player2_acbl_number] = player2_masterpoints

# Map each single player to their elo total.
acbl_num_to_elo = {}
res = conn.execute("select acbl_number, elo from elo")
for acbl_num, player_elo in res:
    acbl_num_to_elo[acbl_num] = player_elo

"""
Load the testing data, shape (n_test, 7)
Columns:
ns1_acbl_number, ns2_acbl_number, ew1_acbl_number, 
ew2_acbl_number, ns_matchpoints, ew_match_points, session_id
"""
X_test = np.load("../data/X_test.npy")

"""
For each testing example, predict the result based on 
(1) Masterpoint totals: (higher "wins")
(2) Elo totals: (higher "wins")
and record the performance.
"""
elo_predictions = [] # 1 = NS win, 0 = EW win.
masterpoint_predictions = [] # 1 = NS win, 0 = EW win.
true_results = [] # 1 = NP win, 0 = EW win.
elo_diffs = [] # NS - EW.
masterpoint_diffs = [] # (NS - EW)
matchpoint_diffs = [] # (NS - EW) / (NS + EW)
print("Predict X test using MP and ELO")
counter = 0
for ns1_acbl_number, ns2_acbl_number, ew1_acbl_number, ew2_acbl_number, ns_matchpoints, ew_matchpoints, session_id in X_test:

    # Counter.
    if counter % 10000 == 0:
        msg = "\t...{}/{}".format(counter, X_test.shape[0])
        print(msg)
    counter += 1

    # Skip draws.
    if ns_matchpoints == ew_matchpoints:
        continue

    # Order ACBL numbers in [ns1, ns2, ew1, ew2] format for easy reuse.
    acbl_numbers = [ns1_acbl_number, ns2_acbl_number, ew1_acbl_number, ew2_acbl_number]

    # Lookup ELO and MP for all 4 players [ns1, ns2, ew1, ew2]
    individual_elos = [acbl_num_to_elo[n] for n in acbl_numbers]
    individual_mps = [acbl_num_to_masterpoints[n] for n in acbl_numbers]

    # Pool ELO and MP by partnership.
    ns_pooled_elo = np.mean([individual_elos[0], individual_elos[1]])
    ew_pooled_elo = np.mean([individual_elos[2], individual_elos[3]])
    ns_pooled_mp = np.mean([individual_mps[0], individual_mps[1]])
    ew_pooled_mp = np.mean([individual_mps[2], individual_mps[3]])

    # Predict winner based on MP and ELO. 1 = NS win.
    elo_prediction = int(ns_pooled_elo > ew_pooled_elo)
    mp_prediction = int(ns_pooled_mp > ew_pooled_mp)

    # Binarize result. 1 = NS win.
    ns_win = int(ns_matchpoints > ew_matchpoints)

    # Record predictions and result.
    elo_predictions.append(elo_prediction)
    masterpoint_predictions.append(mp_prediction)
    true_results.append(ns_win)

    # Also record a few more sensitive statistics.
    elo_diffs.append(ns_pooled_elo - ew_pooled_elo)
    masterpoint_diffs.append(ns_pooled_mp - ew_pooled_mp)
    matchpoint_diffs.append((ns_matchpoints - ew_matchpoints) / (ns_matchpoints + ew_matchpoints))


# Output performance of the two models.
print("score model performance")
masterpoint_accuracy = accuracy_score(masterpoint_predictions, true_results)
elo_accuracy = accuracy_score(elo_predictions, true_results)
n_test = X_test.shape[0] # TODO adjust to factor that we skipped draws.
print(np.mean(elo_predictions))
print(np.mean(masterpoint_predictions))
print(np.mean(true_results))

msg = """
Model accuracy, based on {} test trials:
    - MP: {:.2f} 
    - ELO: {:.2f} 
""".format(n_test, masterpoint_accuracy, elo_accuracy)
print(msg)

# Experimental, a more sensitive evaluation based on amount of difference in elo, masterpoints, and matchpoints.
from scipy.stats import spearmanr
rho_elo, p_elo = spearmanr(elo_diffs, matchpoint_diffs)
rho_masterpoints, p_masterpoints = spearmanr(masterpoint_diffs, matchpoint_diffs)
msg = """
More sensitive analysis using spearman rho of amount of different in ELO or Masterpoints -> amount of difference in matchpoints.
ELO: rho = {:.2f}
Masterpoints: rho = {:.2f}
""".format(rho_elo, rho_masterpoints)
print(msg)
