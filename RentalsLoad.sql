CREATE SCHEMA raw_rentals;


SET VARIABLE path_input = '/Users/arthurtran/Library/CloudStorage/OneDrive-Personnel/Cours/Exploration & Visualisation Données/Projet/BikeShare_Washington/data/';
SELECT getvariable('path_input');


-- Insertion des données avec les colonnes 'start_station_id' et 'end_station_id' en VARCHAR car ID = 76C50D7330BD4ECA pose PB
CREATE OR REPLACE TABLE raw_rentals.rentals_v2 AS
SELECT 
FROM read_csv(
    getvariable('path_input') || 'rentals/V1/*.csv', 
    types={
        'start_station_id': 'VARCHAR',
        'end_station_id': 'VARCHAR'
    }
);


CREATE OR REPLACE TABLE raw_gbfs.station_information AS
SELECT *
FROM read_json('https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_information.json');

-- Insertion des données avec les colonnes 'start_station_id' et 'end_station_id' en VARCHAR car ID = 76C50D7330BD4ECA pose PB
CREATE OR REPLACE TABLE raw_rentals.rentals_v1 AS
SELECT
	"Duration" AS Duration,
    "start date" AS started_at,
    "End date" AS ended_at,
    "Start station number" AS start_station_id,
    "Start station" AS start_station_name,
    "End station number" AS end_station_id,
    "End station" AS end_station_name,
    "Member type" AS member_casual,
    'classic_bike' AS 'Rideable_type'
FROM read_csv(
    getvariable('path_input') || 'rentals/V1/*.csv', 
    types={
        'Start station number': 'VARCHAR',
        'End station number': 'VARCHAR'
    }
);

CREATE OR REPLACE TABLE raw_rentals.rentals_v1_with_coords AS
SELECT
    v1.Duration,
    v1.started_at,
    v1.ended_at,
    v1.start_station_id,
    v1.start_station_name,
    v1.end_station_id,
    v1.end_station_name,
    v1.member_casual,
    v1.rideable_type,
    v2.start_lat AS start_station_lat,
    v2.start_lng AS start_station_lng,
    v2.end_lat AS end_station_lat,
    v2.end_lng AS end_station_lng
FROM raw_rentals.rentals_v1 AS v1
INNER JOIN raw_rentals.rentals_v2 AS v2
    ON v1.start_station_id = v2.start_station_id
    AND v1.end_station_id = v2.end_station_id;

CREATE OR REPLACE TABLE  raw_rentals.rentals_v2 AS
SELECT
	rideable_type,
	started_at,
 	ended_at,
 	end_station_id
	start_station_name,
	datediff('second', started_at, ended_at)  as duration,
	start_station_id,
	end_station_name,
	end_station_id,
	start_lat,
	start_lng,
	end_lat,
	end_lng,
	member_casual
FROM read_csv(
    getvariable('path_input') || 'rentals/V2/*.csv', 
    types={
        'start_station_id': 'VARCHAR',
        'end_station_id': 'VARCHAR'
    }
);

tt