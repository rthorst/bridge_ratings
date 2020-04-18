"""
Table schema.

------
session
------
session_id  club_name   start_date  board_scoring_method

------
board_results
------
result_id   board_id    section_id  session_id  ns_pair ew_pair ns_match_points ew_match_points

------
pairs
------
pair_id session_id  section_id  pair_number direction   session_percentage  player1_acbl_number player2_acbl_number 
player1_name player2_name player1_masterpoints player2_masterpoints

-----
elo
-----
acbl_number      elo         n_hands

"""
import sqlite3
from elo import elo
import numpy as np
from sklearn.model_selection import train_test_split

"""
Create ELO table and put every acbl number from pairs in the table with 1200 ELO and 0 hands.
"""

# Connect to database.
conn = sqlite3.connect("../data/bridge.db")

# Drop elo table if exists.
conn.execute("drop table if exists elo;")

# Create elo table.
create_elo_sql = """
create table if not exists elo (
    acbl_number integer primary key,
    elo numeric,
    n_hands integer
);
"""
conn.execute(create_elo_sql)

# Get all distinct ACBL numbers in the dataset.
get_distinct_acbl_numbers_sql = """
select distinct player1_acbl_number from pairs
union
select distinct player2_acbl_number from pairs;
"""
distinct_acbl_numbers = conn.execute(get_distinct_acbl_numbers_sql)

# Initialize all of these players with 0 elo rating.
print("initialize all players with 1200 ELO and 0 hands played")
for idx, tup in enumerate(distinct_acbl_numbers):

    # counter.
    if idx % 100 == 0:
        print("\t...{}".format(idx))

    acbl_no = tup[0]
    insert_player_sql = """
    insert into elo (acbl_number, elo, n_hands)
    values ({}, 1200, 0);
    """.format(acbl_no)
    conn.execute(insert_player_sql)

"""
Merge board_results and pairs table, so that each player is identified by their ACBL number.
"""

# Merge board_results and pairs tables.
print("load and prepare dataset of played boards")
merge_statement = open("sql/merge_board_results_and_pairs_tables.sql", "r").read()
res = conn.execute(merge_statement)

# Cast the merged board_results and pairs tables to a numpy array, X.
# Shape (nboards, 7)
# Columns: ns1_acbl_number, ns2_acbl_number, ew1_acbl_number, ew2_acbl_number,
#          ns_match_points, ew_match_points, session_id
X = []
for tup in res:
    X.append(list(tup))
X = np.array(X)

"""
Split training, testing data.
"""

# List all session ids.
res = conn.execute("select distinct session_id from session;")
session_ids = []
for tup in res:
    session_ids.append(tup[0])

# Train, test, split.
print("split training, testing data")
test_size = 0.1
session_ids_train, session_ids_test = train_test_split(session_ids, 
        test_size = test_size)

test_mask = np.array([xi in session_ids_test for xi in X[:, 6]])
X_train = X[~test_mask]
X_test = X[test_mask]

print("\t...shapes: train {}, test {}".format(X_train.shape, X_test.shape))

# Persist train, testing data for later.
print("persist train, test data to ../data/X_train.npy, ../data/X_test.npy")
np.save("../data/X_train.npy", X_train)
np.save("../data/X_test.npy", X_test)

"""
Calculate ELO ratings based on historical data.
"""

# Store ELOs in memory, for faster read/write.
# After all data is processed, we will update the SQL database.
print("load player elo ratings in memory, for faster read/write")
res = conn.execute("select acbl_number, elo from elo;")
acbl_num_to_elo = {}
for acbl_num, player_elo in res:
    acbl_num_to_elo[acbl_num] = player_elo

# Iterate over training data.
counter = 0
n_hands = X_train.shape[0]

for ns1_acbl_number, ns2_acbl_number, ew1_acbl_number, ew2_acbl_number, ns_match_points, ew_match_points, session_id in X_train:

    # Type cast X_train[i], as appropriate.
    ns1_acbl_number = int(ns1_acbl_number)
    ns2_acbl_number = int(ns2_acbl_number)
    ew1_acbl_number = int(ew1_acbl_number)
    ew2_acbl_number = int(ew2_acbl_number)
    ns_match_points = float(ns_match_points)
    ew_match_points = float(ew_match_points)
    session_id = int(session_id)
    
    # Counter.
    if counter % 25000 == 0:

        # Print counter.
        msg = "update elo for hand {}/{}".format(counter, n_hands)
        print(msg)

    # Update counter.
    counter += 1

    # Look-up the old elo ratings associated with these players.
    acbl_numbers = [ns1_acbl_number, ns2_acbl_number, ew1_acbl_number, ew2_acbl_number]
    old_elo_ratings = [] # ns1, ns2, ew1, ew2.
    for acbl_number in acbl_numbers:

        # Get elo for this player and append to old_elo_ratings []
        old_elo = acbl_num_to_elo[acbl_number]
        old_elo_ratings.append(old_elo)

    # Average elo ratings for NS and EW.
    ns_pooled_elo = (old_elo_ratings[0] + old_elo_ratings[1]) / 2
    ew_pooled_elo = (old_elo_ratings[2] + old_elo_ratings[3]) / 2

    # Calculate new "pooled" elo for NS and EW.
    if ns_match_points > ew_match_points: # case 1: NS "win"
        new_ns_pooled_elo, new_ew_pooled_elo = elo.rate_1vs1(ns_pooled_elo, ew_pooled_elo, drawn=False)
    elif ew_match_points > ns_match_points: # case 2: EW "win"
        new_ns_pooled_elo, new_ew_pooled_elo = elo.rate_1vs1(ew_pooled_elo, ns_pooled_elo, drawn=False)
    else: # case 3: "draw"
        new_ns_pooled_elo, new_ew_pooled_elo = elo.rate_1vs1(ns_pooled_elo, ew_pooled_elo, drawn=True)

    # Convert this new "pooled" elo for NS and EW into an amount to update.
    # each individual score, e.g. (new_ew_pooled_elo - ew_pooled_elo)
    ew_update = (new_ew_pooled_elo - ew_pooled_elo)
    ns_update = (new_ns_pooled_elo - ns_pooled_elo)

    # Calculate a vector of updates to the old ELO ratings.
    elo_updates = np.array([ns_update, ns_update, ew_update, ew_update])

    # Update elo ratings in the ELO lookup dictinary.
    for acbl_number, elo_update in zip(acbl_numbers, elo_updates):

        acbl_num_to_elo[acbl_number] += elo_update

"""
Update ELO ratings in the ELO table.
"""
print("alter elo SQL table with new ELO ratings")
for acbl_num, player_elo in acbl_num_to_elo.items():

    update_statement = """
    update elo set elo = {} where acbl_number = {};
    """.format(player_elo, acbl_num)

    conn.execute(update_statement)

# Commit changes.
conn.commit()

# Calculate some descriptive statistics of the ELO ratings,
# to manually verify the results are reasonable.
test_sql = """
    select min(elo), max(elo), avg(elo), count(*) from elo;
"""
min_elo, max_elo, avg_elo, n_players = next(conn.execute(test_sql))
msg = """
Done! Check that these numbers look reasonable:
The ELOs range from {:.2f} to {:.2f} 
with a mean of {:.2f} and {} total players rated
""".format(min_elo, max_elo, avg_elo, n_players)
print(msg)



