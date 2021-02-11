import datetime
import json
import psycopg2
import requests

import logger
import utils


def download_and_insert_data(url: str, cursor: psycopg2.extensions.cursor, 
    log: logger.Log) -> None:
    """
    Download player data from `url` in pages, inserting each page
    (a JSON array) into the stage.player_blobs table using `cursor`.
    Log as a metric using `log` the time it took to download all
    the player data, across all pages.
    """
    page = 0
    empty_response = False

    time1 = datetime.datetime.now()

    while not empty_response:
        player_response = utils.make_get_request(f"{url}?page={page}")

        if player_response.content == b'[]':
            empty_response = True
        else:
            content_blob = player_response.json()
            content_blob = json.dumps(content_blob).replace("'", "''")  
            insert_player_blob(cursor, content_blob)

        page += 1

    time2 = datetime.datetime.now()
    log.write_metric('player_download_seconds', (time2 - time1).total_seconds())
  

def insert_player_blob(cursor: psycopg2.extensions.cursor, content_json: str) -> None:
    """
    Using `cursor`, insert `content_json` into the stage.player_blobs table.
    This functionality is split into its own function to enable using a
    test data file.
    """
    cursor.execute(f"INSERT INTO stage.player_blobs(player_blob) VALUES ('{content_json}') ")


def debatch_blob(cursor: psycopg2.extensions.cursor):
    """
    Using `cursor`, split the JSON arrays in stage.player_blobs
    into individual player records and insert them into stage.player_info.
    """
    debatch_sql = """
    INSERT INTO stage.player_info (player_id, details, create_timestamp)
    SELECT player_detail ->> 'id'
    , player_detail
    , create_timestamp
    FROM
    (
    SELECT jsonb_array_elements(player_blob) AS player_detail
    , create_timestamp
    FROM stage.player_blobs
    ) AS a;
    """

    cursor.execute(debatch_sql)


def check_and_mark_data_quality(cursor: psycopg2.extensions.cursor,
    log: logger.Log) -> None:
    """
    Using `cursor`, mark rows in the stage.player_info table that
    satisfy data quality rules. Log to `log` a warning if any rows
    fail data quality.
    """
    quality_check_sql = """
    UPDATE stage.player_info
    SET passed_data_quality_check = true
    WHERE details ? 'data';
    """

    cursor.execute(quality_check_sql)

    cursor.execute("SELECT COUNT(*) FROM stage.player_info WHERE "
        "passed_data_quality_check = false;")
    number_bad = cursor.fetchone()[0]
    if number_bad > 0:
        log.write_warning(f'Rejected {number_bad} player records due to data quality.')


def move_checked_data(cursor: psycopg2.extensions.cursor, 
    retain_staging_data: bool) -> None:
    """
    Using `cursor`, copy rows from stage.player_info to prepared.player_info
    rows that passed the data quality checks. Copy rows that failed the 
    data quality checks from stage.player_info to error.player_info.
    Remove rows from the stage.player_blobs and stage.player_info tables to clean 
    up for the next run unless `retain_staging_data` is True.
    """
    # Copy the rows that passed the data quality check to the `prepared` table
    copy_to_processed_sql = """
    INSERT INTO prepared.player_info (player_id, details, create_timestamp)
    SELECT player_id
    , details -> 'data'
    , create_timestamp
    FROM stage.player_info
    WHERE passed_data_quality_check = true;
    """

    cursor.execute(copy_to_processed_sql)

    # Copy the rows that failed the data quality check to the `problem` table
    copy_to_error_sql = """
    INSERT INTO error.player_info (player_id, details, create_timestamp)
    SELECT player_id
    , details
    , create_timestamp
    FROM stage.player_info
    WHERE passed_data_quality_check = false;
    """
    
    cursor.execute(copy_to_error_sql)

    # Clean out the stage tables for the next run
    if not retain_staging_data:
        cursor.execute('TRUNCATE TABLE stage.player_blobs;')
        cursor.execute('TRUNCATE TABLE stage.player_info;')


def load_data(data_url: str, host: str, port: int, database: str, user: str, 
    password: str, replace_existing_data: bool) -> None:
    """
    Wrapper function for the player pipeline. Download data from `data_url`. 
    Create a database connection using `host`, `port`, `database`, `user`, 
    and `password` and load downloaded data into the stage.player_info table. 
    Move rows that satisfy data quality
    rules to the prepared.player_info table and rows that fail checks to the
    error.player_info table. If `replace_existing_data` is True remove data 
    from the prepared.player_info table before new data is added from the 
    stage.player_info table.
    """
    log = logger.Log()
    log.write_info('Begin load_player_data.load_data')

    connection = None
    
    try:
        connection = utils.make_db_connection(host, port, database, user, password)
        cursor = connection.cursor()

        download_and_insert_data(data_url, cursor, log)
        debatch_blob(cursor)
        check_and_mark_data_quality(cursor,log)

        if replace_existing_data:
            cursor.execute('TRUNCATE TABLE prepared.player_info;')

        move_checked_data(cursor, False)

        cursor.close()
        connection.commit()

    except (requests.exceptions.HTTPError) as error:
        log.write_error(f'There was an error downloading the player data. {error.args}')
    except (psycopg2.OperationalError, psycopg2.Error) as error:
        log.write_error(f'There was a database error. {error.args}')

    finally:
        if connection:
            connection.close()

    log.write_info('End load_player_data.load_data')



