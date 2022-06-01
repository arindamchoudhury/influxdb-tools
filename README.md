# credentials
modify and credential in lib/config.py

# influxdb copy database
./influxdb_copy_database.py --from_db=[db-name] --to_db=[db-name]

# influxdb backup database
## backup is done in separate files in line protocol format. influxdb_{DB_NAME}_{MEASUREMENT_NAME}_{END_TIMESTAMP}.backup

./influxdb_backup_database.py --db=[db-name]
