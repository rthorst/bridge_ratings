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
merge_statement = open("sql/merge_board_results_and_pairs_tables.sql", "r").read()
res = conn.execute(merge_statement)

# count number of hands we will update elo for.
n_hands = conn.execute("select count(*) from board_results")
n_hands = next(n_hands)[0]

"""
Calculate ELO ratings based on historical data.
"""

# For each hand:
counter = 0
for ns1_acbl_number, ns2_acbl_number, ew1_acbl_number, ew2_acbl_number, ns_match_points, ew_match_points in res:

    # Counter.
    if counter % 5 == 0:

        # Add hand number to message.
        msg = "update elo for hand {}/{}".format(counter, n_hands)

        # Get largest ELO so far.
        res = conn.execute("select min(elo), max(elo) from elo;")
        min_elo, max_elo = next(res)
        msg += ": min elo {}, max elo {}".format(min_elo, max_elo)
        
        # Print message.
        print(msg)

        # Update counter.
        counter += 1

    # Look-up the old acbl ratings associated with these players.
    acbl_numbers = [ns1_acbl_number, ns2_acbl_number, ew1_acbl_number, ew2_acbl_number]
    old_elo_ratings = [] # ns1, ns2, ew1, ew2.
    for acbl_number in acbl_numbers:

        # Get elo for this player and append to old_elo_ratings []
        get_elo_sql = "select elo from elo where acbl_number = {};".format(acbl_number)
        old_elo = next(conn.execute(get_elo_sql))[0]
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

    # Calculate a vector of new elo ratings.
    # [ns1_new_elo, ns2_new_elo, ew1_new_elo, ew2_new_elo]
    elo_updates = np.array([ns_update, ns_update, ew_update, ew_update])
    old_elo_ratings = np.array(old_elo_ratings)
    new_elo_ratings = elo_updates + old_elo_ratings

    # Update elo ratings in the ELO table.
    for acbl_number, new_elo in zip(acbl_numbers, new_elo_ratings):

        update_rating_sql = """
        update elo
        set elo = {}
        where acbl_number = {};
        """.format(new_elo, acbl_number)
        conn.execute(update_rating_sql)
        conn.commit()

    





