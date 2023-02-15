import sqlite3
import click
import os
import pandas as pd
from datetime import datetime
from flask import current_app, g

directory_in_str = str(os.path.dirname(__file__)) + "\wx_data"
directory = os.fsencode(directory_in_str)


def get_db():
    """Function to get db object so that we dont have to establish db connection each time when db processsing is needed"""
    if "db" not in g:
        g.db = sqlite3.connect(
            current_app.config["DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row
    return g.db

def close_db(e=None):
    """Function to close the db"""
    db = g.pop("db", None)
    if db is not None:
        db.close()

def init_db():
    """Function to initialize the the tables in db"""
    db = get_db()
    with current_app.open_resource("schema.sql") as f:
        db.executescript(f.read().decode("utf8"))


def process_txt_data(count_file, count_row, filename):
    """Function to process the text data and insert into wx_data table using pandas
        Input:
        count_file -> count of the files being processed
        count_row -> count of rows ingested
        filename -> file being worked 
        
        Output:
        count_file, count_row -> final numbers"""
    count_file += 1
    station = filename.replace(
        ".txt", ""
    ) 
    # Use the name of the file as Station Name
    # Read each file and push to DB
    file_path = os.path.join(directory_in_str, filename)
    # convert the txt file to txt_df so that data manipulation is easy
    column_names = ["date", "max_temp", "min_temp", "tot_prec"]
    txt_df = pd.read_csv(
        file_path, sep="\t", names=column_names, header=None, index_col=False
    )
    txt_df["station"] = station
    txt_df = txt_df.drop_duplicates()  # Drop duplicates
    txt_df.reset_index(drop=True, inplace=True)  # Drop index
    txt_df = txt_df[["station", "date", "max_temp", "min_temp", "tot_prec"]]
    count_row += txt_df.shape[0]  # Get number of rows of the db

    # Insert into DB now
    db_conn = get_db()
    txt_df.to_sql(name="WX_DATA", con=db_conn, if_exists="append", index=False)

    return count_file, count_row

def summarize_data():
    """Function to summarize the ingested data into stats and inserting into wx_stats table"""
    db = get_db()
    if not db:
        return False
    cursor = db.cursor()
    insert_avg_query = """
                    INSERT INTO WX_STATS
                    SELECT STATION,
                            SUBSTR(DATE,1,4) AS YEAR,
                            AVG(NULLIF(MAX_TEMP, -9999)) AS AVG_MAX_TEMP,
                            AVG(NULLIF(MIN_TEMP, -9999)) AS AVG_MIN_TEMP,
                            AVG(NULLIF(TOT_PREC, -9999)) AS AVG_TOT_PREC
                    FROM WX_DATA 
                    GROUP BY STATION, SUBSTR(DATE,1,4)
                    ;
                        """
    cursor.execute(insert_avg_query)
    db.commit()
    cursor.close()

# Creates command line comamnd to initialize db and return success message
@click.command("init-db")
def init_db_command():
    init_db()
    click.echo("DATABASE INITIALIZED")

@click.command("close-db")
def init_close_command():
    close_db()
    click.echo("DATABASE CLOSED")

# Creates command line comamnd to insert wx data into db and return success message
@click.command("init-data")
def init_data_command():
    # Get the count of the files processed
    count_file = 0
    # Get the count of the rows inserted in DB
    count_row = 0
    # Check if the data folder exists
    if not os.path.exists(directory):
        current_app.logger.info("There is no data folder")
    # Check if the data folder is empty or not
    if not os.listdir(directory):
        current_app.logger.info("There is no files in the folder")
    
    current_app.logger.info(
        "DATA INGESTION STARTED AT "
        + datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S %f")
    )

    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename.endswith(".txt"):
            count_file, count_row = process_txt_data(count_file, count_row, filename)

    if count_file == 0:
        current_app.logger.info("There were no .txt files in the folder")
    else:
        current_app.logger.info(f"{count_file} txt files were executed")
        current_app.logger.info(f"{count_row} rows were inserted in the DB")

    current_app.logger.info(
        "DATA INGESTION STOPPED AT "
        + datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S %f")
    )

@click.command("summarize-data")
def init_summarise_command():
    summarize_data()

# Initiate above functions with application instance so that it can be used by it.
def init_app(app):
    app.teardown_appcontext(close_db)  # calls the close_db func when returning respose
    app.cli.add_command(init_db_command)  # adds new command that flask can use to initialize the db and tables
    app.cli.add_command(init_data_command)  # adds new command that flask can use to start ingesting the data
    app.cli.add_command(init_close_command)  # adds new command that flask can use to close the db
    app.cli.add_command(init_summarise_command)  # adds new command that flask can use to summarize the ingested data into stats

