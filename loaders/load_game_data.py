import datetime
import os
import psycopg2
import requests

import logger
import utils


def download_data(url: str, local_csv_path: str, log: logger.Log) -> None:
    """
    Download the data from `url` and write it to the file 
    specified by `local_csv_path`. Log using `log` the time taken to 
    download as a metric.
    """
    time1 = datetime.datetime.now()
    content = utils.make_get_request(url).content
    time2 = datetime.datetime.now()
    log.write_metric('game_download_seconds', (time2 - time1).total_seconds())
    open(local_csv_path, 'wb').write(content)


def load_staging_table(local_csv_path: str, cursor: psycopg2.extensions.cursor) -> None:
    """
    Copy data from the file at `local_csv_path` into the 
    stage.game_data table using `cursor`.
    """
    with open(local_csv_path, 'r') as f:
        next(f) # Skip the header line
        cursor.copy_from(f, 'stage.game_data', 
            columns=['game_id', 'player_id', 'move_number', '"column"', 'result'], 
            sep=',')


def check_and_mark_data_quality(cursor: psycopg2.extensions.cursor,
    log: logger.Log) -> None:
    """
    Using `cursor`, mark rows in the stage.game_data table that
    satisfy data quality rules. Log to `log` a warning if any rows
    fail data quality.
    """
    quality_check_sql = '''
    UPDATE stage.game_data 
    SET passed_data_quality_check = True 
    WHERE game_id NOT IN
    (
    SELECT distinct(game_id)
    FROM stage.game_data
    WHERE move_number ~ '\D' -- Must be an integer
    OR "column" ~ '\D' -- Must be an integer
    OR "column"::int > 4  -- The grid is 4x4
    OR result NOT IN ('', 'win', 'draw') -- Valid values for result
    )
    AND game_id NOT IN
    (
    SELECT game_id
    FROM stage.game_data
    GROUP BY game_id
    HAVING COUNT(DISTINCT(player_id)) <> 2  -- A game should have 2 players
    );
    '''
    cursor.execute(quality_check_sql)

    cursor.execute("SELECT COUNT(*) FROM stage.game_data WHERE "
        "passed_data_quality_check = false;")
    number_bad = cursor.fetchone()[0]
    if number_bad > 0:
        log.write_warning(f'Rejected {number_bad} game records due to data quality.')

def move_checked_data(cursor: psycopg2.extensions.cursor, 
    retain_staging_data: bool) -> None:
    """
    Using `cursor`, copy rows from stage.game_data to prepared.game_data
    rows that passed the data quality checks. Copy rows that failed the 
    data quality checks from stage.game_data to error.game_data.
    Remove rows from the stage.game_data table to clean up for the next
    run unless `retain_staging_data` is True.
    """
    # Copy the rows that passed the data quality check to the `prepared` table
    copy_to_prepared_sql = '''
    INSERT INTO prepared.game_data (game_id, player_id, move_number, "column", result, create_timestamp) 
    SELECT game_id, player_id, move_number::int, "column"::int, result, create_timestamp
    FROM stage.game_data WHERE passed_data_quality_check = True;
    '''
    cursor.execute(copy_to_prepared_sql)

    # Copy the rows that failed the data quality check to the `problem` table
    copy_to_error_sql = '''
    INSERT INTO error.game_data (game_id, player_id, move_number, "column", result, create_timestamp) 
    SELECT game_id, player_id, move_number, "column", result, create_timestamp
    FROM stage.game_data WHERE passed_data_quality_check = False;
    '''
    cursor.execute(copy_to_error_sql)

    # Clean out the stage table for the next run
    if not retain_staging_data:
        cursor.execute('TRUNCATE TABLE stage.game_data;')


def load_data(data_url: str, local_csv_path: str, retain_csv_file: bool,
    host: str, port: int, database: str, user: str, password: str, 
    replace_existing_data: bool) -> None:
    """
    Wrapper function for the game pipeline. Download data from `data_url`
    to a file at `local_csv_path`. Create a database connection using
    `host`, `port`, `database`, `user`, and `password` and load downloaded
    data into the stage.game_data table. Move rows that satisfy data quality
    rules to the prepared.game_data table and rows that fail checks to the
    error.game_data table. Delete the download file unless `retain_csv_file`
    is True. If `replace_existing_data` is True remove data from the 
    prepared.game_data table before new data is added from the stage.game_data 
    table.
    """
    log = logger.Log()
    log.write_info('Begin load_game_data.load_data')

    try:
        download_data(data_url, local_csv_path, log)

    except (requests.exceptions.HTTPError) as error:
        log.write_error(f'There was an error downloading the CSV file. {error.args}')

    connection = None

    try:
        connection = utils.make_db_connection(host, port, database, user, password)
        cursor = connection.cursor()

        load_staging_table(local_csv_path, cursor)
        check_and_mark_data_quality(cursor, log)

        if replace_existing_data:
            cursor.execute('TRUNCATE TABLE prepared.game_data;')
            
        move_checked_data(cursor, False)

        cursor.close()
        connection.commit()

    except (psycopg2.OperationalError, psycopg2.Error) as error:
        log.write_error(f'There was a database error. {error.args}')

    finally:
        if connection:
            connection.close()

    if not retain_csv_file:
        os.remove(local_csv_path)

    log.write_info(f'End load_game_data.load_data')

  
