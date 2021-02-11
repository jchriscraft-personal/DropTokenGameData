## Overview
This application uses Python version 3 to load Drop Token game and player data
into a PostgreSQL database and provides views that support the three homework project analyses. 
Data ingestion follows the ELT pattern, where the extracted data is loaded into the database 
and then transformed using SQL statements.

Tables in the `stage` schema hold the raw data extracted from the sources. Data validation
queries are run, and compliant records are transformed and moved to the `prepared` schema
where they are made available to views in the `reporting` schema. Records that fail the 
data quality checks are moved to the `error` schema. Loading assumes that full game 
and player data sets are retrieved on each run. The `prepared` tables have their data 
fully replaced each time.

## Setup
1. Install Python if it is not already installed. This application was written to use Python version 3.8. That
can be installed for MacOS and Windows from https://www.python.org/downloads/release/python-380/.  
See https://docs.python-guide.org/starting/install3/linux/ for a good resource on installing Python on Linux.

2. This project uses `pipenv` to create the virtual environment. Install `pipenv` for Python 3 by running:  
`pip3 install pipenv`

3. Create the virtual environment for this project. In a terminal window, change directories to the root of this
project. Be in the root `DropTokenGameData` directory. From there, run this command:  
`pipenv sync`

4. Identify a PostgreSQL database server. If needed, PostgreSQL can be installed following instructions at https://www.postgresql.org/download/. PostgreSQL version 12 was used to develop this project.

5. With a database user that can create a database, connect to the PostgreSQL server. Create a database to be used by this application. The project configuration assumes the database name `drop_token`, but use any desired name.  
You can do this visually through a tool like pgAdmin, or you can run the following `psql` command to create the database.    
Update the parameter values as appropriate for your enivornment and database user.  
`psql -h localhost -p 5432 -d postgres -U postgres -c "CREATE DATABASE drop_token;"`


6. In the new database, load the content of the `DW_setup.sql` file located in the root of the project. This script
will create schemas, tables, and views.  
Note that at the bottom of this script are the views used for the three assignment analysis questions. These are noted with SQL comments in the script.   
As with the previous step, you can do this visually via pgAdmin or through a psql command like the one below. Update the
parameter values to match your environment and the location of the `DW_setup.sql` file.  
`psql -h localhost -p 5432 -d drop_token -U postgres -f "DW_setup.sql"`

7. Edit the `configuration.yml` file in the root of the project. Set a value for the first 5 entries--the database related settings. 
- `database_server`: set the PostgreSQL database server host name
- `database_server_port`: set the PostgreSQL database server port. The default port (5432) is pre-configured.
- `database`: update the default value (`drop_token`) if you used a different database name in step #5.
- `database_user` and `database_password`: set the credentials for a Postgres user than can read, write, and truncate tables in the database.
  
8. Save the configuration file.

## Running the application
To run the application to ingest the real game and player data, use a terminal set at the root of this project, at the top level `DropTokenGameData` directory. Run:  
`pipenv run ./load_game_and_player_data.py`


Find the `log.txt` file that gets created at the root of this project. Look for any entries that begin with `ERROR:`. If there are any errors, use the information logged to resolve issues.

If there were no errors, the game and player data was ingested into the database indicated in the `configuration.yml` file. Connect to that database and query the views in the `reporting` schema to explore the data.

## Running tests
This project uses the Python `unittest` framework to run tests that exercise the functionality of the
data ingestion code and the SQL views. Tests use the data files in the `TestData` directory.  
Execute the following command in a terminal set at the root of this project.
Note that running the tests will empty all the tables in the database.  
`pipenv run ./tests.py`

The output in the terminal should indicate that 5 tests ran and will end with the word `OK` if all tests passed. The file `test_log.txt` at the root of the project gets generated to record information from the setup of the test environment.

## Empty All Tables
The simple `empty_all_tables.py` file at the root of the project does just that--it removes data from all the tables
used by the application. This can be useful for testing. In terminal at the project root run:  
`pipenv run ./empty_all_tables.py`



