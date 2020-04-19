import os
import csv
import bs4
import requests 
import re
import time
import slimit
import json 
import sqlite3

def get_links_to_individual_game_records():
    """
    Populates a list of URLs, each with the game summary for a single game.
    
    Input: 
        ../data/club-summary-urls.txt, which for now is manually created.
        (each URL lists many games)

    Output: 
        ../data/game-summary-urls.txt  :  one url per line.
        (each URL is a single game from the above)
      """

    # List website URLs containing club summaries.
    summary_urls = open("../data/club-summary-urls.txt", "r").readlines()
    summary_urls = [url for url in summary_urls if len(url) > 5]
    print(summary_urls)

    # Extract links to individual games.
    game_urls = []
    for summary_url in summary_urls:

        # Fetch game summary html.
        summary_url = summary_url.replace("\n", "")
        summary_html = requests.get(summary_url).text
      
        # Soupify html.
        soup = bs4.BeautifulSoup(summary_html)

        # Extract links to individual games.
        attrs = {"href" : re.compile("details")}
        game_summary_links = soup.findAll("a", attrs=attrs)
        game_summary_links = [link.get("href") for link in game_summary_links]       
        # Convert these links from relative to absolute urls.
        base_url = r"https://my.acbl.org/"
        game_summary_links = [base_url + link for link in game_summary_links]
        
        # Add links from this page to all links.
        game_urls.extend(game_summary_links)

    # Write.
    with open("../data/game-summary-urls.txt", "w", newline="") as of:
        for url in game_urls:
            of.write(url + "\n")
    print("done, wrote game-summary-urls.txt")


def get_html_of_individual_games():
    """
    Download the raw HTML for records of individual club games.

    Input: ../data/game-summary-urls.txt 

    Output: ../data/game_htmls/*

    """

    # create output directory if not exists.
    of_p = "../data/game_htmls"
    if not os.path.exists(of_p):
        os.mkdir(of_p)

    # list urls to download.
    urls_to_download = open("../data/game-summary-urls.txt").readlines()
    urls_to_download = [url.rstrip("\n") for url in urls_to_download if len(url) > 5]

    # download individual games.
    n = len(urls_to_download)
    for idx, url in enumerate(urls_to_download):

        try:
            
            # download game.
            print("\t...{} / {}".format(idx, n))
            html = requests.get(url).text
            with open("../data/game_htmls/{}.htm".format(idx), "w") as of:
                of.write(html)   

            # sleep.
            time.sleep(.1)
            
        except Exception as e:
            print(e)

    # done.
    print("done, wrote data/game_htmls/*")

def parse_game_records():
    """
    Parse bridge game records into several shared SQL tables
    stored in ../data/bridge.db

    Tables created:
        session
        pairs
        board_results

    For table schemas, see the create table statements which are exposed
    below. 
    """

    """
    Create SQL databas and tables. 
    """
    # Create database and tables.
    conn = sqlite3.connect("../data/bridge.db")
    table_names = ["session", "pairs", "board_results"]
    for table_name in table_names:
        conn.execute("drop table if exists {};".format(table_name))

    # Create session table.
    create_session_sql = """
    create table session (
        session_id  integer primary key,
        club_name varchar,
        start_date varchar,
        board_scoring_method varchar
    );
    """
    conn.execute(create_session_sql)

    # Create boards table.
    create_boards_sql = """
    create table board_results (
        result_id integer primary key,
        board_id integer,
        section_id integer,
        session_id integer,
        ns_pair integer,
        ew_pair integer,
        ns_match_points numeric,
        ew_match_points numeric);
    """
    conn.execute(create_boards_sql)

    # Create pairs table.
    create_pairs_sql = """
    create table pairs (
        pair_id integer primary key,
        session_id integer,
        section_id integer,
        pair_number integer,
        direction varchar,
        session_percentage numeric,
        player1_acbl_number integer,
        player2_acbl_number integer,
        player1_name varchar,
        player2_name varchar,
        player1_masterpoints numeric,
        player2_masterpoints numeric
    );
    """
    conn.execute(create_pairs_sql)
    
    """
    Extract data from game htmls
    """
    for html_fname in os.listdir("../data/game_htmls"):

        try:

            # Load game HTML
            html_p = "../data/game_htmls/{}".format(html_fname)
            print(html_p)
            game_html = open(html_p, "r").read()
            soup = bs4.BeautifulSoup(game_html, "html.parser")

            # Extract data object.
            script_with_data_tag = soup.findAll("script")[-2]
            data_s = re.findall("var data =(.+?);\n", str(script_with_data_tag), re.S)[0]
            data_s = data_s.rstrip(";\n")
           
            # Parse javascript data -> dictionary type.
            data_s = data_s.lstrip("var data = ")
            j = json.loads(data_s)   

            """
            Write data to SQL tables.
            """

            # Add to session table.
            club_name = j["club_name"]
            start_date = j["start_date"]
            board_scoring_method = j["board_scoring_method"]
            for session in j["sessions"]:
                session_id = session["id"]

                session_insert_sql = """
                insert into session (session_id, club_name, start_date, board_scoring_method) values
                ({}, '{}', '{}', '{}');
                """.format(session_id, club_name, start_date, board_scoring_method)
                conn.execute(session_insert_sql)

            # Add to board results table.
            for session in j["sessions"]:
                session_id = session["id"]

                for section in session["sections"]:
                    section_id = section["id"]

                    for board in section["boards"]:
                        board_results = board["board_results"]
                        
                        for result in board_results:

                            result_id = result["id"]
                            board_id = result["board_id"]
                            ns_pair = result["ns_pair"]
                            ew_pair = result["ew_pair"]
                            ns_match_points = result["ns_match_points"]
                            ew_match_points = result["ew_match_points"]

                            board_insert_sql = """
                            insert into board_results (result_id, board_id, section_id, session_id, ns_pair, ew_pair, ns_match_points, ew_match_points)
                            values ({}, {}, {}, {}, {}, {}, {}, {});
                            """.format(result_id, board_id, section_id, session_id, ns_pair, ew_pair, ns_match_points, ew_match_points)
                            conn.execute(board_insert_sql)



            for session in j["sessions"]:
                session_id = session["id"]

                for section in session["sections"]:
                    section_id = section["id"]

                    for pair_summary in section["pair_summaries"]:
                        percentage = pair_summary["percentage"]
                        direction = pair_summary["direction"]
                        pair_id = pair_summary["id"]
                        pair_number = pair_summary["pair_number"]

                        player1_acbl_number = pair_summary["players"][0]["id_number"]
                        player2_acbl_number = pair_summary["players"][1]["id_number"]
                        if 'tmp' in str(player1_acbl_number):
                            print("skip pair with 1+ missing acbl number")
                            continue
                        if 'tmp' in str(player2_acbl_number):
                            print("skip pair with 1+ missing acbl number")
                            continue

                        player1_name = pair_summary["players"][0]["name"]
                        player2_name = pair_summary["players"][1]["name"]

                        player1_masterpoints = pair_summary["players"][0]["mp_total"]
                        player2_masterpoints = pair_summary["players"][1]["mp_total"]

                        pairs_insert_sql = """
                        insert into pairs (session_id, section_id, pair_id, pair_number, direction, session_percentage, player1_acbl_number, player2_acbl_number, player1_name, player2_name, player1_masterpoints, player2_masterpoints) values ({}, {}, {}, {}, '{}', {}, {}, {}, '{}', '{}', {}, {});
                        """.format(session_id, section_id, pair_id, pair_number, direction, percentage, player1_acbl_number, player2_acbl_number, player1_name, player2_name, player1_masterpoints, player2_masterpoints)
                        conn.execute(pairs_insert_sql)

        except Exception as e:
            print(e)

    conn.commit() # commit changes.

    """ 
    Show tables.
    """
    for table_name in ["session", "pairs", "board_results"]:
        table_len = conn.execute("select count(*) from {}".format(table_name))
        msg = "{} : len = {}".format(table_name, next(table_len))
        print(msg)
        res = conn.execute("select * from {} limit 5;".format(table_name))
        print("-----------")
        for tup in res:
            print(tup)

    

if __name__ == "__main__":
    #get_links_to_individual_game_records()
    #get_html_of_individual_games()
    parse_game_records()
