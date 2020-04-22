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

    # Create boards table.
    create_boards_sql = """
    create table board_results (
        result_id integer,
        ns1_acbl_number integer,
        ns2_acbl_number integer,
        ew1_acbl_number integer,
        ew2_acbl_number integer,
        ns_match_points numeric,
        ew_match_points numeric);
    """
    conn.execute(create_boards_sql)

    
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

            # Keep only open pairs games.
            # Thus, skip if this game is not open pairs.
            # In pilot experiments, when all flights were included, it was difficult
            # for the ELO model to learn to down-rank limited flight players, due to 
            # sectioning of players in certain games. Thus low rank players could gain an
            # unreasonably high elo by never leaving non-open games.
            #if not 'open pairs' in game_html.lower():
            #    print("skip {} which is not open pairs".format(html_p))
            #    continue

            # Extract data object.
            script_with_data_tag = soup.findAll("script")[-2]
            data_s = re.findall("var data =(.+?);\n", str(script_with_data_tag), re.S)[0]
            data_s = data_s.rstrip(";\n")
           
            # Parse javascript data -> dictionary type.
            data_s = data_s.lstrip("var data = ")
            j = json.loads(data_s)   

            """
            Write data to SQL tables.
            We write one simple table with each board result and simply acbl numbers plus matchpoint totals.
            """

            # Add to board results table.
            for session in j["sessions"]:
                session_id = session["id"]

                for section in session["sections"]:
                    section_id = section["id"]

                    # Map pair numbers in this section to player ACBL numbers.
                    pair_num_and_direction_to_acbl_nums = {} # key e.g. "7_NS" values = [acblnum1, acblnum2]
                    for pair_summary in section["pair_summaries"]:

                        # Create key (pairNum_section).
                        pair_num = pair_summary["pair_number"]
                        direction = pair_summary["direction"]
                        k = pair_num + "_" + direction

                        # List player acbl numbers.
                        acbl_nums = [pair_summary["players"][0]["id_number"], pair_summary["players"][1]["id_number"]]

                        # Add this pair to the dictionary.
                        pair_num_and_direction_to_acbl_nums[k] = acbl_nums

                    print(pair_num_and_direction_to_acbl_nums)

                    # Get individual board results.
                    # Note that NS pair and EW pair are duplicated IDs across directions, eg. there is an 8NS and an 8EW, etc.
                    for board in section["boards"]:
                        board_results = board["board_results"]
                        
                        for result in board_results:

                            result_id = result["id"]
                            board_id = result["board_id"]
                            ns_pair = result["ns_pair"]
                            ew_pair = result["ew_pair"]
                            ns_match_points = result["ns_match_points"]
                            ew_match_points = result["ew_match_points"]

                            # lookup acbl numbers for the players.
                            ns1_acbl_number, ns2_acbl_number = pair_num_and_direction_to_acbl_nums[ns_pair + "_" + "NS"]
                            ew1_acbl_number, ew2_acbl_number = pair_num_and_direction_to_acbl_nums[ew_pair + "_" + "EW"]

                            board_insert_sql = """
                            insert into board_results (result_id, ns1_acbl_number, ns2_acbl_number, ew1_acbl_number, ew2_acbl_number, ns_match_points, ew_match_points)
                            values ({}, {}, {}, {}, {}, {}, {})
                            """.format(result_id, ns1_acbl_number, ns2_acbl_number, ew1_acbl_number, ew2_acbl_number, ns_match_points, ew_match_points)

                            conn.execute(board_insert_sql)
                        
        except Exception as e:
            print(e)

    conn.commit() # commit changes.

    """ 
    Show tables.
    """
    for table_name in ["board_results"]:
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
