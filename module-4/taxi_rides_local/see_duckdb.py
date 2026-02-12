import duckdb

con = duckdb.connect("taxi_rides_local.duckdb")
taxi_type = "green"
print(con.execute(f"SELECT COUNT(*) FROM taxi_rides_local.prod.{taxi_type}_tripdata").df())

con.close()