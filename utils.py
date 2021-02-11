import psycopg2
import requests
from typing import Any, Dict
from yaml import load, FullLoader

def make_db_connection(host: str, port: int, database: str, user: str, password: str) \
    -> psycopg2.extensions.connection:
    return psycopg2.connect(f'host={host} port={port} dbname={database} \
        user={user} password={password}')


def make_db_connection_from_config(config: Dict[Any, Any]) -> psycopg2.extensions.connection:
    return make_db_connection(config['database_server'], 
        config['database_server_port'], config['database'], config['database_user'], 
        config['database_password'])


def load_configuration(config_file: str) -> Dict[Any, Any]:
    with open(config_file, 'r') as stream:
        config = load(stream, Loader=FullLoader)
    return config


def make_get_request(url: str, raise_for_status: bool = True) -> requests.Response:
    response = requests.get(url, allow_redirects=True)
    if raise_for_status:
        response.raise_for_status()
    return response


def empty_all_tables(cursor: psycopg2.extensions.cursor) -> None:
    tables = ['stage.game_data', 'error.game_data', 'prepared.game_data', 'stage.player_blobs',
    'stage.player_info', 'error.player_info', 'prepared.player_info']

    for table in tables:
        cursor.execute(f'TRUNCATE TABLE {table};')
