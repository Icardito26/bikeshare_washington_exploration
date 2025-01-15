-- ----------------------------------------------------------
-- Déclaration des variables path_input pour le chemin d'accès aux fichiers d'entrée
-- ----------------------------------------------------------

-- Windows : "C:\Users\arthu\OneDrive\Cours\Exploration & Visualisation Données\Projet\BikeShare_Washington\data\"
-- Mac : "/Users/arthurtran/Library/CloudStorage/OneDrive-Personnel/Cours/Exploration & Visualisation Données/Projet/BikeShare_Washington/data/"

SET VARIABLE path_input = '/Users/arthurtran/Library/CloudStorage/OneDrive-Personnel/Cours/Exploration & Visualisation Données/Projet/BikeShare_Washington/data/';
SELECT getvariable('path_input');


-- ----------------------------------------------------------
-- Essai d'insertion des données brutes GBFS
-- ----------------------------------------------------------

CREATE SCHEMA raw_gbfs;

CREATE OR REPLACE TABLE raw_gbfs.station_information AS
SELECT *
FROM read_json('https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_information.json');


-- ----------------------------------------------------------
-- Insertion :  Raw data
-- ----------------------------------------------------------

-- Création du schéma raw_rentals
CREATE SCHEMA raw_rentals;

-- Insertion des données brutes depuis des fichiers CSV avec le modèle V1
-- Modification du type des colonnes 'Start station number' et 'End station number' en VARCHAR car certaines valeurs sont des caractères
create or replace table raw_rentals.raw_rental_v1 as 
 SELECT *  FROM read_csv(
    getvariable('path_input') || 'rentals/V1/*.csv', 
    types={
        'Start station number': 'VARCHAR',
        'End station number': 'VARCHAR'
    }
);

-- Insertion des données brutes depuis des fichiers CSV avec le modèle V2
-- Modification du type des colonnes 'Start station number' et 'End station number' en VARCHAR car certaines valeurs sont des caractères
create or replace table raw_rentals.raw_rental_v2 as 
 SELECT * 
 FROM read_csv(
    getvariable('path_input') || 'rentals/V2/*.csv', 
    types={
        'start_station_id': 'VARCHAR',
        'end_station_id': 'VARCHAR'
    }
);


----------------------------------------------
-- Insertion :  Staging data
----------------------------------------------

-- Création du schéma stg
CREATE SCHEMA stg;

/*
Création de la table stg.rentals à partir des tables raw_rentals.raw_rental_v1 et raw_rentals.raw_rental_v2
Les colonnes ont été renommées pour être homogènes
Les colonnes start_lat, start_lng, end_lat, end_lng ont été ajoutées et initialisées à null pour être homogènes avec la table raw_rentals.raw_rental_v2
La colonne "bikeid" a été supprimée car elle n'est pas présente dans la table raw_rentals.raw_rental_v2 et n'est pas utilisée dans le reste du projet
La colonne "ride_id" a été supprimée car elle n'est pas présente dans la table raw_rentals.raw_rental_v1 et n'est pas utilisée dans le reste du projet
*/

create or replace table stg.rentals as 
SELECT
    NULL AS rideable_type,
    --null as rideable_type
    "start date" AS started_at,
    "End date" AS ended_at,
    "Start station number" AS start_station_id,
    "Start station" AS start_station_name,
    "End station number" AS end_station_id,
    "End station" AS end_station_name,
    null as start_lat,
    null as start_lng,
    null as end_lat,
    null as end_lng,
	"Duration" AS Duration,
    "Member type" AS member_casual,
  -- bikeid

from raw_rentals.raw_rental_v1
union all

SELECT
	-- ride_id,
	rideable_type,
	started_at,
	ended_at,
	start_station_id,
	start_station_name,
	end_station_id,
	end_station_name,
	start_lat,
	start_lng,
	end_lat,
	end_lng,
	datediff('second', started_at, ended_at) as duration,
	member_casual,
FROM raw_rentals.raw_rental_v2;



----------------------------------------------
-- explo rapido 
----------------------------------------------
SELECT 
  --YEAR(started_at) AS annee, -- Année de location
  -- MONTH(started_at) AS mois, -- Mois de location(année confondue)
  -- datetrunc('month', started_at) dt_mois,
  STRFTIME('%Y-%m', started_at) AS dt_mois,  
  COUNT(1) AS nb_rentals, -- Nbr total de locations
  COUNT(DISTINCT start_station_id) AS nb_stations, -- Nbr de stations de départ
  COUNT(DISTINCT start_station_name) AS nb_station_name -- Nbr de NOMS de stations de départ
FROM stg.rentals
GROUP BY all
ORDER BY 1; 

----------------------------------------------
-- offload rapido : Exportation 
----------------------------------------------
COPY stg.rentals TO 'C:\Users\ziton\OneDrive\Bureau\BikeShare_Washington\output\rentals.parquet' 
  (FORMAT PARQUET); 

COPY stg.rentals TO 'C:\Users\ziton\OneDrive\Bureau\BikeShare_Washington\output\rentals.csv' 
  (FORMAT CSV);
 
select count(*)
from stg.rentals;


--------------------------------
--md
--------------------------------
CREATE SCHEMA md;
   
CREATE OR REPLACE TABLE md.rentals AS 
SELECT
    rideable_type,
    started_at,
    ended_at,
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    duration,
    member_casual
FROM stg.rentals;


UPDATE md.rentals r1
SET
    start_lat = (SELECT start_lat 
                 FROM stg.rentals r2 
                 WHERE r1.start_station_id = r2.start_station_id 
                 AND r1.end_station_id = r2.end_station_id 
                 AND r2.start_lat IS NOT NULL
                 LIMIT 1),  -- Limite les résultats à 1 ligne
    start_lng = (SELECT start_lng 
                 FROM stg.rentals r2 
                 WHERE r1.start_station_id = r2.start_station_id 
                 AND r1.end_station_id = r2.end_station_id 
                 AND r2.start_lng IS NOT NULL
                 LIMIT 1),
    end_lat = (SELECT end_lat 
               FROM stg.rentals r2 
               WHERE r1.start_station_id = r2.start_station_id 
               AND r1.end_station_id = r2.end_station_id 
               AND r2.end_lat IS NOT NULL
               LIMIT 1),
    end_lng = (SELECT end_lng 
               FROM stg.rentals r2 
               WHERE r1.start_station_id = r2.start_station_id 
               AND r1.end_station_id = r2.end_station_id 
               AND r2.end_lng IS NOT NULL
               LIMIT 1)
WHERE r1.start_lat IS NULL
   OR r1.start_lng IS NULL
   OR r1.end_lat IS NULL
   OR r1.end_lng IS NULL;



select count(*)
from md.rentals;