import sqlite3
from elo import elo
import numpy as np
from sklearn.model_selection import train_test_split

"""
Create an SQL table, elo, to hold estiamted ELO ratings for each player.
We initialize each player with a baseline 1200 ELO.
"""

# Connect to existing SQL database: ../data/bridge.db
conn = sqlite3.connect("../data/bridge.db")

# If the ELO table already exists, drop it. Create from scratch.
conn.execute("drop table if exists elo;")

# Create the ELO table.
create_elo_sql = """
create table if not exists elo (
    acbl_number integer primary key,
    elo numeric,
    n_hands integer
);
"""
conn.execute(create_elo_sql)

# List all distinct ACBL numbers in the dataset, in preparation for 
# assigning these players a baseline 1200 ELO.
get_distinct_acbl_numbers_sql = """
select distinct player1_acbl_number from pairs
union
select distinct player2_acbl_number from pairs;
"""
distinct_acbl_numbers = conn.execute(get_distinct_acbl_numbers_sql)

# Assign each player a baseline 1200 ELO to start.
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
To train the ELO model, we need to associate results of a single board with
metadata about the players, such as their ACBL number.

Here, we create such a table by merging the pairs and board_results tables.
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

# Since we will split training and testing data by session id, 
# start by listing all session IDs.
res = conn.execute("select distinct session_id from session;")
session_ids = []
for tup in res:
    session_ids.append(tup[0])

# Split training and testing data by session ID.
print("split training, testing data")
test_size = 0.1
session_ids_train, session_ids_test = train_test_split(session_ids, 
        test_size = test_size)

test_mask = np.array([xi in session_ids_test for xi in X[:, 6]])
X_train = X[~test_mask]
X_test = X[test_mask]

print("\t...shapes: train {}, test {}".format(X_train.shape, X_test.shape))

# Save training and testing data , for later use.
print("persist train, test data to ../data/X_train.npy, ../data/X_test.npy")
np.save("../data/X_train.npy", X_train)
np.save("../data/X_test.npy", X_test)

"""
Train the ELO model. 

To do this, iterate over hands of historical data (in X_train)
and update the ELO model after each hand
"""

# Since reading/writing many times to a SQL table is slow, 
# begin by storing all ELO data in MEMORY.
# we train the model based on this in-memory ELO table, 
# then update the resulting ELOs after training
print("load player elo ratings in memory, for faster read/write")
res = conn.execute("select acbl_number, elo from elo;")
acbl_num_to_elo = {}
for acbl_num, player_elo in res:
    acbl_num_to_elo[acbl_num] = player_elo

# Iterate over training data.
counter = 0
n_hands = X_train.shape[0]

for ns1_acbl_number, ns2_acbl_number, ew1_acbl_number, ew2_acbl_number, ns_match_points, ew_match_points, session_id in X_train:

    # Ensure all data is of the proper type through explicit casting.
    ns1_acbl_number = int(ns1_acbl_number)
    ns2_acbl_number = int(ns2_acbl_number)
    ew1_acbl_number = int(ew1_acbl_number)
    ew2_acbl_number = int(ew2_acbl_number)
    ns_match_points = float(ns_match_points)
    ew_match_points = float(ew_match_points)
    session_id = int(session_id)
    
    # Periodically output progress to the console.
    if counter % 25000 == 0:

        # Print counter.
        msg = "update elo for hand {}/{}".format(counter, n_hands)
        print(msg)

    counter += 1

    # Query the old ELO ratings associated with these players.
    acbl_numbers = [ns1_acbl_number, ns2_acbl_number, ew1_acbl_number, ew2_acbl_number]
    old_elo_ratings = [] # ns1, ns2, ew1, ew2.
    for acbl_number in acbl_numbers:

        # Get elo for this player and append to old_elo_ratings []
        old_elo = acbl_num_to_elo[acbl_number]
        old_elo_ratings.append(old_elo)

    # Pool the ELO ratings for each partnership, to get a sense
    # of partnership strength.
    ns_pooled_elo = (old_elo_ratings[0] + old_elo_ratings[1]) / 2
    ew_pooled_elo = (old_elo_ratings[2] + old_elo_ratings[3]) / 2

    # Calculate an update to these pooled ELO ratings, based on 
    # the result and its surprisingness, given the existing
    # elo ratings.
    if ns_match_points > ew_match_points: # case 1: NS "win"
        new_ns_pooled_elo, new_ew_pooled_elo = elo.rate_1vs1(ns_pooled_elo, ew_pooled_elo, drawn=False)
    elif ew_match_points > ns_match_points: # case 2: EW "win"
        new_ns_pooled_elo, new_ew_pooled_elo = elo.rate_1vs1(ew_pooled_elo, ns_pooled_elo, drawn=False)
    else: # case 3: "draw"
        new_ns_pooled_elo, new_ew_pooled_elo = elo.rate_1vs1(ns_pooled_elo, ew_pooled_elo, drawn=True)

    # Vectorize the amount to update each existing ELO rating.
    ew_update = (new_ew_pooled_elo - ew_pooled_elo)
    ns_update = (new_ns_pooled_elo - ns_pooled_elo)

    elo_updates = np.array([ns_update, ns_update, ew_update, ew_update])

    # Update elo ratings in the ELO lookup dictinary.
    for acbl_number, elo_update in zip(acbl_numbers, elo_updates):

        acbl_num_to_elo[acbl_number] += elo_update

"""
Now that model training is complete, update the SQL-based elo
table with the new ratings, which for now are simply stored in memory.
"""

print("Update elo table with the new ratings, computed in-memory")
for acbl_num, player_elo in acbl_num_to_elo.items():

    update_statement = """
    update elo set elo = {} where acbl_number = {};
    """.format(player_elo, acbl_num)

    conn.execute(update_statement)

# Explicitly commit changes to the SQL table, which is necessary to
# save the changes.
conn.commit()

# As an "eye test", output basic descriptive statistics for the 
# elo ratings as stored in the SQL database. If these look good, 
# then all was a success.
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



