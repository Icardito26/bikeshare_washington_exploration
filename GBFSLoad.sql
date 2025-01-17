-- ----------------------------------------------------------
-- Insertion : Données GBFS
-- ----------------------------------------------------------

-- Installation Spatial pour les points
install spatial;load spatial;

-- Création schéma raw_GBFS
CREATE SCHEMA raw_gbfs;

-- Création schéma stg
CREATE SCHEMA stg;

-- Variable pour chemin dossier Data et GBFS
set VARIABLE path_input = 'C:\Users\arthu\OneDrive\Cours\Exploration & Visualisation Données\Projet\BikeShare_Washington\data\';
SELECT getvariable('path_input');


-- Essai avec : Donnée du monde
create table raw_gbfs.earth_systems as
from 'https://raw.githubusercontent.com/MobilityData/gbfs/refs/heads/master/systems.csv';

select *, lower(location) location_lower
from raw_gbfs.earth_systems
where location_lower like '%washington%'
  or location_lower like '%paris%'
  
 
-- ----------------------------------------------------------
-- Exploration des données pour Washington et Paris
-- ----------------------------------------------------------
 select *
 from read_json('https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json')
 
 -- Washington :
create or replace table raw_gbfs.station_information as 
 select
	'washington' ville,
	--data::json "data", --> on va plutôt directement extraire les infos des stations
	data.stations::json[] stations,
	last_updated,
	ttl,
	null as version
FROM 'https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_information.json';

-- Paris : 
insert into raw_gbfs.station_information
 select
	'paris' ville,
	data.stations::json[] stations,
	lastUpdatedOther as last_updated,
	ttl,
	null as version
FROM 'https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json';


-- ----------------------------------------------------------
-- Création vue pour donnée stg concernant GBFS Station information
-- ----------------------------------------------------------

create or replace view stg.gbfs_station_information as
WITH stations AS (
    SELECT
        ville,
        unnest(stations) AS station,
        to_timestamp(last_updated)::TIMESTAMPTZ AT TIME ZONE 
            (CASE 
                WHEN ville = 'washington' THEN 'America/New_York'  
                ELSE 'Europe/Paris' 
            END) AS last_updated
    FROM raw_gbfs.station_information
)
SELECT
    ville,
    station->>'station_id' AS station_id,
    station->>'name' AS name,
    (station->>'capacity')::int AS capacity,
    (station->>'lon')::numeric AS longitude,
    (station->>'lat')::numeric AS latitude,
    last_updated
FROM stations;



----------------------------------------------------
-- Insert into station_status
----------------------------------------------------

create or replace table raw_gbfs.station_status as
select 'washington' ville, data.stations::json[] stations, last_updated, ttl, null as version
FROM 'https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_status.json';

insert into raw_gbfs.station_status
select 'paris' ville, data.stations::json[] stations, lastUpdatedOther as last_updated,	ttl, null as version
FROM 'https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json';

-- est ce que j'ai bien ingéré ??
select *
from stg.gbfs_station_status
order by ville, station_id


-- ----------------------------------------------------------
-- Création vue pour donnée stg concernant GBFS Station status
-- ----------------------------------------------------------

create or replace view stg.gbfs_station_status as
WITH stations AS (
    SELECT
        ville,
        unnest(stations) AS station,
        to_timestamp(last_updated)::TIMESTAMPTZ AT TIME ZONE 
            (CASE 
                WHEN ville = 'washington' THEN 'America/New_York'  
                ELSE 'Europe/Paris' 
            END) AS last_updated
    FROM raw_gbfs.station_status
)
SELECT
	ville,
	station->>'station_id' station_id,
	(station->'num_bikes_available')::int num_bikes_available,
	(station->'capacity')::int capacity,
	last_updated,
from stations;

-- ----------------------------------------------------------
-- Création table MD
-- ----------------------------------------------------------

create or replace table md.gbfs_station_information as
Select *
From stg.gbfs_station_information gsi; 

create or replace table md.gbfs_station_status as
Select *
From stg.gbfs_station_status gsi; 

----------------------------------------------
-- Exportation donnée du schéma MD
----------------------------------------------

COPY md.gbfs_station_information TO 'C:\Users\arthu\OneDrive\Cours\Exploration & Visualisation Données\Projet\BikeShare_Washington\output\md_gbfs_station_information.parquet' 
  (FORMAT PARQUET); 
 
COPY md.gbfs_station_status TO 'C:\Users\arthu\OneDrive\Cours\Exploration & Visualisation Données\Projet\BikeShare_Washington\output\md_gbfs_station_status.parquet' 
  (FORMAT PARQUET); 


  