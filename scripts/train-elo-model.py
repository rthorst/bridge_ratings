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

"""
Create ELO table and put every acbl number from pairs in the table with 1200 ELO and 0 hands.
"""

# Connect to database.
conn = sqlite3.connect("../data/bridge.db")

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

#res = conn.execute(merge_board_results_pairs_sql)
#print(next(res))

"""
Calculate ELO ratings based on Atlanta historical data.
"""
s = "select session_id from session;"
res = conn.execute(s)
print(next(res))
# Foreach hand, do.
# Lookup all ACBL numbers.
# Binarize the result: NS win, EW win, draw.
# Update Elo ratings and put in table.

# Commit changes.




