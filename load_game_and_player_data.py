#! /usr/bin/env python3
#
# This script downloads and ingests Drop Token game
# and player data into the database configured in
# the `configuration.yml` file located in the same
# directory. The database and its tables must be created
# using the `DW_setup.sql` script (located in the same directory)
# before running this script.
# The script assumes that both the games and players
# data sets are complete data sets each time the script runs.
# Data in the `prepared` schema tables is removed before
# new data gets added.
from loaders import load_game_data as games, load_player_data as players
import utils

local_games_csv_path = './game_data.csv'

config = utils.load_configuration('./configuration.yml')

games.load_data(config['game_data_csv_location'], local_games_csv_path, False,
    config['database_server'], config['database_server_port'], config['database'],
    config['database_user'], config['database_password'], True)

players.load_data(config['player_data_location'], config['database_server'], 
    config['database_server_port'], config['database'], config['database_user'], 
    config['database_password'], True)





