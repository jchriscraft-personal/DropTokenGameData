-- The `stage` schema holds raw data
CREATE SCHEMA stage;

CREATE TABLE stage.game_data
(
 game_id text
, player_id text
, move_number text
, "column" text
, result text
, create_timestamp timestamp DEFAULT NOW()
, passed_data_quality_check bool DEFAULT False
);

CREATE TABLE stage.player_blobs
(
 player_blob jsonb
, create_timestamp timestamp DEFAULT NOW()
);

CREATE TABLE stage.player_info
(
 player_id text
 , details jsonb
 , create_timestamp timestamp
 , passed_data_quality_check bool DEFAULT False
);

-- The `prepared` schema holds data that has passed quality checks
-- and has been prepared for consumption
CREATE SCHEMA prepared;

CREATE TABLE prepared.game_data
(
 game_id text
, player_id text
, move_number int
, "column" int
, result text
, create_timestamp timestamp
);

CREATE INDEX ix_game_data_game_id ON prepared.game_data (game_id);
CREATE INDEX ix_game_data_move_number ON prepared.game_data (move_number);
CREATE INDEX ix_game_dataresult ON prepared.game_data (result);

CREATE TABLE prepared.player_info
(
 player_id text
 , details jsonb
 , create_timestamp timestamp
);

CREATE INDEX idx_btree_hobbies ON prepared.player_info USING GIN (details jsonb_ops);

-- The `error` schema holds data that failed quality checks
CREATE SCHEMA error;

CREATE TABLE error.game_data
(
 game_id text
, player_id text
, move_number text
, "column" text
, result text
, create_timestamp timestamp
);

CREATE TABLE error.player_info
(
 player_id text
 , details jsonb
 , create_timestamp timestamp
);

-- The `reporting` schema exposes objects that can be consumed for reporting
CREATE SCHEMA reporting;

CREATE VIEW reporting.player_game
AS
(
SELECT DISTINCT game_id, player_id
FROM prepared.game_data   
);

CREATE VIEW reporting.game_summary AS
(
SELECT game_id
, concluding_moves.move_number AS total_moves
, initial_moves.player_id AS initial_player
, concluding_moves.player_id AS concluding_player
, initial_moves.column AS initial_column
, concluding_moves.column AS concluding_column
, concluding_moves.result AS result
, CASE WHEN concluding_moves.result = 'win' THEN concluding_moves.player_id ELSE null END AS winner
, CASE WHEN concluding_moves.result = 'win' THEN 
(SELECT player_id FROM reporting.player_game WHERE game_id = concluding_moves.game_id 
 	AND player_id <> concluding_moves.player_id)
ELSE null END AS loser
FROM prepared.game_data initial_moves
JOIN prepared.game_data concluding_moves USING (game_id)
WHERE initial_moves.move_number = 1
AND concluding_moves.result <> ''
);

CREATE VIEW reporting.player_details AS
(
SELECT player_id
, details ->> 'nat' as nationality
, details ->> 'email' as email_address
, details
FROM prepared.player_info
);

--- Views to answer the interview analyses ---

-- Analysis 1: When the first player is choosing a
-- column for their first move, which column most frequently leads to 
-- that player winning the game?
-- Implementation note: only consider games in which the first player
-- is also the last player and the game results in a 'win'.
CREATE VIEW reporting.winning_initial_column
AS
(
WITH qualifying_games AS
(
SELECT initial_column
FROM reporting.game_summary
WHERE concluding_player = initial_player and result = 'win'
)

SELECT initial_column
, initial_column_game_count
, (SELECT COUNT(*) FROM qualifying_games) AS total_game_count
, ROUND(initial_column_game_count * 100.0 / (SELECT COUNT(*) FROM qualifying_games), 2) AS percent_of_total
FROM
(
SELECT initial_column
, COUNT(*) AS initial_column_game_count
FROM qualifying_games
GROUP BY initial_column
) AS a
ORDER BY initial_column_game_count DESC
);

-- Analysis 2: How many games has each nationality participated in?
-- Implementation note: If two players from the same country play 
-- in the same game, count that as only one participation.
CREATE VIEW reporting.nationality_participation
AS 
(
SELECT nationality, COUNT(DISTINCT(game_id)) AS game_count
FROM reporting.player_game pg
JOIN reporting.player_details pd USING (player_id)
GROUP BY nationality
ORDER BY nationality
);

-- Analysis 3: Marketing wants to email players who have played
-- only one game. Customize the message based on whether they
-- won, lost, or there was a draw.
CREATE VIEW reporting.single_game_player AS
(
SELECT pg.player_id
, pg.game_id
, pd.email_address
, pd.nationality
, CASE WHEN pg.player_id = gs.winner THEN 'won'
WHEN pg.player_id = gs.loser THEN 'lost'
ELSE 'drew' END AS player_outcome
FROM reporting.player_game pg
JOIN reporting.game_summary gs ON pg.game_id = gs.game_id
JOIN reporting.player_details pd ON pg.player_id = pd.player_id
WHERE pg.player_id IN
(
SELECT player_id
FROM reporting.player_game
GROUP BY player_id
HAVING COUNT(DISTINCT(game_id)) = 1
)
AND pd.email_address IS NOT NULL
);