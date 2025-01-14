CREATE SCHEMA raw_rentals;


SET VARIABLE path_input = '/Users/arthurtran/Library/CloudStorage/OneDrive-Personnel/Cours/Exploration & Visualisation Données/Projet/data/';
SELECT getvariable('path_input');

SELECT * FROM read_csv('/Users/arthurtran/Library/CloudStorage/OneDrive-Personnel/Cours/Exploration & Visualisation Données/Projet/data/rentals/202412-capitalbikeshare-tripdata.csv');

CREATE OR REPLACE TABLE raw_rentals.rentals_v1 AS
SELECT * 
FROM read_csv(getvariable('path_input') || '/rentals/*.csv');



CREATE OR REPLACE TABLE raw_gbfs.station_information AS
SELECT *
FROM read_json('https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_information.json');

SELECT TSSSS