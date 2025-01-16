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
set VARIABLE path_input = '/Users/arthurtran/Library/CloudStorage/OneDrive-Personnel/Cours/Exploration & Visualisation Données/Projet/BikeShare_Washington/data';
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


----------------------------------------------------
-- Insert into station_information
----------------------------------------------------

create or replace table raw_gbfs.station_information as
select 'washigton' ville, data.stations::json[] stations, last_updated, ttl, null as version
FROM 'https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_information.json';

insert into raw_gbfs.station_information
select 'paris' ville, data.stations::json[] stations, lastUpdatedOther as last_updated,	ttl, null as version
FROM 'https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json';

-- est ce que j'ai bien ingéré ??
select *
from stg.gbfs_station_information
order by ville, station_id


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
            END) AS last_updated,
        ttl,
        version
    FROM raw_gbfs.station_information
)
SELECT
    ville,
    station->>'station_id' AS station_id,
    station->>'name' AS name,
    null as num_bikes_available,
    (station->'capacity')::int AS capacity,
    ST_Point((station->'lon')::numeric, (station->'lat')::numeric) AS geom_point,
    station AS raw,
    last_updated,
    ttl,
    version
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
            END) AS last_updated,
        ttl,
        version
    FROM raw_gbfs.station_status
)
SELECT
	ville,
	station->>'station_id' station_id,
	station->>'name' "name",
	(station->'num_bikes_available')::int num_bikes_available,
	(station->'capacity')::int capacity,
	ST_point((station->'lon')::numeric, (station->'lat')::numeric) geom_point,
	station raw,
	last_updated,
	ttl,
	version
from stations;

------------------------------------------------
-- UNION de status et info
------------------------------------------------

create or replace view stg.gbfs_station as
SELECT
    ville,
    station_id,
    name,
    num_bikes_available,
    capacity,
    geom_point,
    raw,
    last_updated,
    ttl,
    version
FROM stg.gbfs_station_status

UNION 

SELECT
    ville,
    station_id,
    name,
    num_bikes_available, -- NULL par défaut dans stg.gbfs_station_information
    capacity,
    geom_point,
    raw,
    last_updated,
    ttl,
    version
FROM stg.gbfs_station_information;


----------------------------------------------
-- Magasin de données pour donnée GBFS
----------------------------------------------

CREATE OR REPLACE TABLE md.gbfs AS 
SELECT
    ville,
    station_id,
    name,
    num_bikes_available,
    capacity,
    geom_point,
    last_updated,
    ttl,
    version
FROM stg.gbfs_station;

UPDATE md.gbfs r1
SET
    name = (SELECT name 
            FROM md.gbfs r2 
            WHERE r1.station_id = r2.station_id 
              AND r2.name IS NOT NULL
            LIMIT 1),
    capacity = (SELECT capacity 
                FROM md.gbfs r2 
                WHERE r1.station_id = r2.station_id 
                  AND r2.capacity IS NOT NULL
                LIMIT 1),
    geom_point = (SELECT geom_point 
                  FROM md.gbfs r2 
                  WHERE r1.station_id = r2.station_id 
                    AND r2.geom_point IS NOT NULL
                  LIMIT 1)
WHERE name IS NULL
   OR capacity IS NULL
   OR geom_point IS NULL;
  
UPDATE md.gbfs r1
SET
    num_bikes_available = (SELECT num_bikes_available
                           FROM md.gbfs r2
                           WHERE r1.station_id = r2.station_id
                             AND r2.num_bikes_available IS NOT NULL
                           LIMIT 1)
WHERE num_bikes_available IS NULL;


CREATE OR REPLACE TABLE md.gbfs_clean AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY last_updated DESC) AS row_num
    FROM md.gbfs
) subquery
WHERE row_num = 1;

DROP TABLE md.gbfs;
ALTER TABLE md.gbfs_clean RENAME TO gbfs;


-------------------------
--vérifier des doublons
-------------------------

SELECT station_id, COUNT(*)
FROM md.gbfs
GROUP BY station_id
HAVING COUNT(*) > 1;


----------------------------------------------
-- Exportation donnée du schéma MD
----------------------------------------------

COPY md.gbfs TO '/Users/arthurtran/Library/CloudStorage/OneDrive-Personnel/Cours/Exploration & Visualisation Données/Projet/BikeShare_Washington/output/md_GBFS.parquet' 
  (FORMAT PARQUET); 

COPY md.rentals TO '/Users/arthurtran/Library/CloudStorage/OneDrive-Personnel/Cours/Exploration & Visualisation Données/Projet/BikeShare_Washington/output/md_rentals.parquet' 
  (FORMAT PARQUET);
  