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

conn = sqlite3.connect("../data/bridge.db")

# Map each single player to their elo total.
acbl_num_to_elo = {}
res = conn.execute("select acbl_number, elo from elo")
for acbl_num, player_elo in res:
    acbl_num_to_elo[acbl_num] = player_elo

"""
Load the testing data, shape (n_test, 7)
Columns:
ns1_acbl_number, ns2_acbl_number, ew1_acbl_number, 
ew2_acbl_number, ns_matchpoints, ew_match_points
"""
X_test = np.load("../data/X_test.npy")

"""
For each testing example, predict the result based on 
(1) Masterpoint totals: (higher "wins")
(2) Elo totals: (higher "wins")
and record the performance.
"""
elo_predictions = [] # 1 = NS win, 0 = EW win.
elo_diffs = [] # NS - EW.
matchpoint_diffs = [] # NS - EW.
true_results = [] # 1 = NP win, 0 = EW win.
counter = 0

for ns1_acbl_number, ns2_acbl_number, ew1_acbl_number, ew2_acbl_number, ns_matchpoints, ew_matchpoints in X_test:

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

    # Pool ELO and MP by partnership.
    ns_pooled_elo = np.mean([individual_elos[0], individual_elos[1]])
    ew_pooled_elo = np.mean([individual_elos[2], individual_elos[3]])

    # Predict winner based on MP and ELO. 1 = NS win.
    elo_prediction = int(ns_pooled_elo > ew_pooled_elo)

    # Binarize result. 1 = NS win.
    ns_win = int(ns_matchpoints > ew_matchpoints)

    # Record predictions and result.
    elo_predictions.append(elo_prediction)
    true_results.append(ns_win)
    elo_diffs.append(ns_pooled_elo - ew_pooled_elo)
    matchpoint_diffs.append((ns_matchpoints - ew_matchpoints))

# Output performance of the two models.
print("score model performance")
elo_accuracy = accuracy_score(elo_predictions, true_results)
n_test = X_test.shape[0] # TODO adjust to factor that we skipped draws.
print(np.mean(elo_predictions))
print(np.mean(true_results))

msg = """
Model accuracy, based on {} test trials:
    - ELO: {:.2f} 
""".format(n_test, elo_accuracy)
print(msg)

# Experimental : statistics that factor the magnitude of difference in elo.
from scipy.stats import spearmanr
rho, p = spearmanr(elo_diffs, matchpoint_diffs)
print(rho, p)
