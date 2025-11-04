# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "371b8757-d19f-4086-9311-40ac7fe1bd5a",
# META       "default_lakehouse_name": "Chess_ETL_Lakehouse",
# META       "default_lakehouse_workspace_id": "ec0bc726-c102-4eb7-8bfc-31cd080a474a",
# META       "known_lakehouses": [
# META         {
# META           "id": "371b8757-d19f-4086-9311-40ac7fe1bd5a"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "90211809-6069-9793-4a8d-0f88e7f3233f",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

username="Rhythmbear1"
month="01"
year="2025"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

! pip install duckdb

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def add_move_numbers(pgn: list) -> str:
    """
    Adds move numbers to a list of chess moves in PGN format.
    Args:
        pgn (list): List of chess moves.
    Returns:
        str: PGN string with move numbers added.
    Example:
        Input: ["e4", "e5", "Nf3", "Nc6", "Bb5"]
        Output: "1. e4 e5 2. Nf3 Nc6 3. Bb5"
    """
    moves = pgn
    
    # Reconstruct the PGN with move numbers
    formatted_pgn = []
    move_number = 1
    for i in range(0, len(pgn), 2):
        # Add the move number and the two moves (white and black)
        formatted_pgn.append(f"{move_number}. {moves[i]} {moves[i+1] if i+1 < len(moves) else ''}")
        move_number += 1
    
    # Join the formatted moves into a single string
    return ' '.join(formatted_pgn)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import duckdb
import json
import pandas as pd

file_path = f'/lakehouse/default/Files/Bronze/{year}/{month}/games.json'
output_path = f'/lakehouse/default/Files/Silver/{year}/{month}/games.parquet'
data = []

with open(file_path, "r") as f:
    data = json.load(f)


games = data['games']
# print(games)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

df = pd.DataFrame(games)
con = duckdb.connect(database=':memory:')

con.create_function('add_move_numbers', add_move_numbers)

# 2. Query the 'games_list' variable directly using the FROM clause
# DuckDB will automatically treat it as a table.
# We'll use .df() to get the result back as a Pandas DataFrame.
result_df = con.execute("FROM df").df()

display(result_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


fct = con.sql(
        """SELECT url as game_url,
            time_control as time_control,
            rated as rated,
            time_class as time_class,
            rules as rules,
            white.rating as white_rating,
            white.result as white_result,
            black.rating as black_rating,
            black.result as black_result,    
            REGEXP_EXTRACT(pgn, '\[Event "(.*?)"', 1) as event,
            REGEXP_EXTRACT(pgn, '\[Site "(.*?)"', 1) as site,
            STRPTIME(REPLACE(REGEXP_EXTRACT(pgn, '\[Date "(.*?)"', 1), '.', '/'), '%Y/%m/%d')::DATE AS game_date, 
            REGEXP_EXTRACT(pgn, '\[White "(.*?)"', 1) as white_user,   
            REGEXP_EXTRACT(pgn, '\[Black "(.*?)"', 1) as black_user,
            REGEXP_EXTRACT(pgn, '\[Result "(.*?)"', 1) as result,
            REGEXP_EXTRACT(pgn, '\[CurrentPosition "(.*?)"', 1) as current_position,
            REGEXP_EXTRACT(pgn, '\[Timezone "(.*?)"', 1) as timezone,
            REGEXP_EXTRACT(pgn, '\[ECO "(.*?)"', 1) as eco,
            REGEXP_EXTRACT(pgn, '\[ECOUrl "(.*?)"', 1) as eco_url,
            STRPTIME(REGEXP_EXTRACT(pgn, '\[StartTime "(.*?)"', 1), '%H:%M:%S'):: TIME as start_time,
            STRPTIME(REGEXP_EXTRACT(pgn, '\[EndTime "(.*?)"', 1), '%H:%M:%S'):: TIME as end_time,
                            STRPTIME(REPLACE(REGEXP_EXTRACT(pgn, '\[EndDate "(.*?)"', 1), '.', '/'), '%Y/%m/%d')::DATE AS end_game_date,
            ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(pgn, '\. (.*?) {\[', 1), ' ') as pgn_raw,
            add_move_numbers(REGEXP_EXTRACT_ALL(pgn, '\. (.*?) {\[', 1)) as pgn_trans"""
        + f" FROM df"
    ).fetchdf()
display(fct)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# import pandas as pd

# Ensure Data accuracy by ensuring that the dates are in the correct format.
# fct["start_time"] = pd.to_datetime(fct["game_date"].astype(str) + " " + fct["start_time"].astype(str),
#     format="%Y-%m-%d %H:%M:%S",
# )
# fct["end_time"] = pd.to_datetime(fct["end_game_date"].astype(str) + " " + fct["end_time"].astype(str),
#     format="%Y-%m-%d %H:%M:%S",
# )

# Upload the file to silver layer

fct.to_parquet(output_path, index=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

con.sql(f"""
    COPY
    (SELECT * FROM fct)
    TO '{output_path}'
    (FORMAT parquet);
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
