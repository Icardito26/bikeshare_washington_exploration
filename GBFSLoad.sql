-- ----------------------------------------------------------
-- Insertion : Données GBFS
-- ----------------------------------------------------------

-- Création schéma raw_GBFS
CREATE SCHEMA raw_gbfs;

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