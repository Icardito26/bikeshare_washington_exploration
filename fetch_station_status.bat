%~d0
cd %~dp0

duckdb Projet_Dbeaver.db "INSERT INTO raw.station_status select 'washington' as ville, ""data"".stations::json[] stations, last_updated, ttl, version FROM 'https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_status.json'"
duckdb bikeshare.db "INSERT INTO raw.station_status select 'montreal' as ville, ""data"".stations::json[] stations, last_updated, ttl, null as version FROM 'https://gbfs.velobixi.com/gbfs/fr/station_status.json'"
