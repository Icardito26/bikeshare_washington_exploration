%~d0
cd %~dp0

duckdb db_bikeshare.db "INSERT INTO raw_gbfs.station_information select 'washigton' ville, data.stations::json[] stations, last_updated, ttl, null as version FROM 'https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_information.json'"
duckdb db_bikeshare.db "INSERT INTO raw_gbfs.station_information select 'paris' ville, data.stations::json[] stations, lastUpdatedOther as last_updated, ttl, null as version FROM 'https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json'"