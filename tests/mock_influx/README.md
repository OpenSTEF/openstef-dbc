# Running the mock influx database

## Install
Make sure you have docker and docker compose installed locally

# Run

Start the database by running `docker compose up` from the current directory

1. Create bucket by running `influx bucket create -n realised/autogen -o "$DOCKER_INFLUXDB_INIT_ORG" -t "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"` in the terminal of the running container.

2. Run `test_write_forecast.py` 

3. Check if measurements are writen by loggin in to the influx ui on `http://localhost:8086` using `usernameonlyfortesting` as username and `passwordpasswordonlyfortesting` as password
