#! /usr/bin/env python3
#
# This is a simple script that removes data from
# all the tables used by the game and player pipelines.
# This script can be useful when testing.
import psycopg2

import logger
import utils

log = logger.Log()

config = utils.load_configuration('./configuration.yml')

connection = None

try:
    connection = utils.make_db_connection_from_config(config)
    cursor = connection.cursor()
    utils.empty_all_tables(cursor)
    cursor.close()
    connection.commit()

except (psycopg2.OperationalError, psycopg2.Error) as error:
        log.write_error(f'There was a database error. {error.args}')

finally:
    if connection:
        connection.close()



 



