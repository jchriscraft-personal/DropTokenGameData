#! /usr/bin/env python3
#
# This script contains unit tests for the games and player
# pipelines.
# `setUpClass` ingests the games and players test data files
# located in the `TestData` folder in this project.
# Test cases verify the data quality checks for each pipeline
# and validate the data returned by some `reporting` views.
# `tearDownClass` cleans out all tables after the tests run.
import unittest

from loaders import load_game_data as games, load_player_data as players
import logger
import utils

games_test_data = './TestData/test_game_data.csv'
test_log_file = './test_log.txt'
config = utils.load_configuration('./configuration.yml')


class Tests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        log = logger.Log(test_log_file)
        log.write_info("Begin Tests.setUpClass")

        connection = utils.make_db_connection_from_config(config)

        try:
            cursor = connection.cursor()
            utils.empty_all_tables(cursor)

            # Load games test data
            games.load_staging_table(games_test_data, cursor)
            games.check_and_mark_data_quality(cursor, log)
            games.move_checked_data(cursor, True)

            # Load players test data
            with open('./TestData/test_player_blob_data.json', 'r') as f:
                player_json = f.read()
                players.insert_player_blob(cursor, player_json)

            players.debatch_blob(cursor)
            players.check_and_mark_data_quality(cursor, log)
            players.move_checked_data(cursor, True)

            cursor.close()
            connection.commit()

        finally:
            if connection:
                connection.close()

        log.write_info("End Tests.setUpClass")


    @classmethod
    def tearDownClass(cls):
        connection = utils.make_db_connection_from_config(config)

        try:
            cursor = connection.cursor()
            utils.empty_all_tables(cursor)
            cursor.close()
            connection.commit()

        finally:
            if connection:
                connection.close()


    def test_game_data_quality(self):
        connection = utils.make_db_connection_from_config(config)

        try:
            cursor = connection.cursor()

            cursor.execute('SELECT DISTINCT(game_id)::int FROM error.game_data ORDER BY game_id::int;')
            results = cursor.fetchall()
            game_ids = list(map(lambda r: r[0], results))
            self.assertEqual([1, 2, 3, 4, 18], game_ids)

            cursor.execute('SELECT DISTINCT(game_id)::int FROM prepared.game_data ORDER BY game_id::int;')
            results = cursor.fetchall()
            game_ids = list(map(lambda r: r[0], results))
            self.assertEqual([5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 19], game_ids)
            
            cursor.close()

        finally:
            if connection:
                connection.close()


    def test_player_data_quality(self):
        connection = utils.make_db_connection_from_config(config)

        try:
            cursor = connection.cursor()

            cursor.execute('SELECT player_id::int FROM error.player_info ORDER BY player_id::int;')
            results = cursor.fetchall()
            player_ids = list(map(lambda r: r[0], results))
            self.assertEqual([0], player_ids)

            cursor.execute('SELECT player_id::int FROM prepared.player_info ORDER BY player_id::int;')
            results = cursor.fetchall()
            player_ids = list(map(lambda r: r[0], results))
            self.assertEqual([101, 102, 103, 104, 105, 106, 107, 108], player_ids)
            
            cursor.close()

        finally:
            if connection:
                connection.close()


    def test_winning_initial_column(self):
        connection = utils.make_db_connection_from_config(config)

        try:
            cursor = connection.cursor()

            cursor.execute('SELECT initial_column, initial_column_game_count, '
                'total_game_count, percent_of_total FROM reporting.winning_initial_column '
                'ORDER BY percent_of_total DESC;')
            results = cursor.fetchall()
            initial_columns = list(map(lambda r: r[0], results))
            initial_column_game_count = list(map(lambda r: r[1], results))
            total_game_count = list(map(lambda r: r[2], results))
            percent_of_total = list(map(lambda r: r[3], results))

            self.assertEqual([4, 3, 2, 1], initial_columns)
            self.assertEqual([4, 3, 2, 1], initial_column_game_count)
            self.assertEqual([10, 10, 10, 10], total_game_count)
            self.assertEqual([40.00, 30.00, 20.00, 10.00], percent_of_total)
 
            cursor.close()

        finally:
            if connection:
                connection.close()


    def test_nationality_participation(self):
        connection = utils.make_db_connection_from_config(config)

        try:
            cursor = connection.cursor()

            cursor.execute('SELECT nationality, game_count FROM '
                'reporting.nationality_participation ORDER BY nationality;')
            results = cursor.fetchall()
            nationality = list(map(lambda r: r[0], results))
            game_count = list(map(lambda r: r[1], results))

            self.assertEqual(['AU', 'CH', 'ES', 'GB', 'IE', 'NZ', 'TR'], nationality)
            self.assertEqual([1, 1, 10, 10, 1, 1, 3], game_count)
 
            cursor.close()

        finally:
            if connection:
                connection.close()


    def test_single_game_player(self):
        connection = utils.make_db_connection_from_config(config)

        try:
            cursor = connection.cursor()

            cursor.execute('SELECT player_id::int, game_id::int, '
            'email_address, nationality, player_outcome '
            'FROM reporting.single_game_player ORDER BY player_id::int;')
            results = cursor.fetchall()
            player_id = list(map(lambda r: r[0], results))
            game_id = list(map(lambda r: r[1], results))
            email_address = list(map(lambda r: r[2], results))
            nationality = list(map(lambda r: r[3], results))
            player_outcome = list(map(lambda r: r[4], results))

            self.assertEqual([103, 105, 106, 107, 108], player_id)
            self.assertEqual([6, 7, 8, 8, 19], game_id)
            self.assertEqual(['auguste.fernandez@example.com', 'latife.akan@example.com', \
                'charles.harris@example.com', 'sofia.vasquez@example.com', \
                'wayne.simpson@example.com'], email_address)
            self.assertEqual(['CH', 'TR', 'NZ', 'AU', 'IE'], nationality)
            self.assertEqual(['won', 'lost', 'won', 'lost', 'drew'], player_outcome)
 
            cursor.close()

        finally:
            if connection:
                connection.close()


if __name__ == '__main__':
    unittest.main()


