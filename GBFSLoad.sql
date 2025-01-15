-- ----------------------------------------------------------
-- Insertion : Données GBFS
-- ----------------------------------------------------------

-- Création schéma raw_GBFS
CREATE SCHEMA raw_gbfs;

-- Variable pour chemin dossier Data et GBFS
set VARIABLE path_input = '/Users/arthurtran/Library/CloudStorage/OneDrive-Personnel/Cours/Exploration & Visualisation Données/Projet/BikeShare_Washington/data';
SELECT getvariable('path_input');