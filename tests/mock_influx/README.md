# Running the mock influx database

## Install
Make sure you have docker and docker compose installed locally

# Run

1. Start the database by running `docker compose up` from the current directory

2. Run `test_write_forecast.py`

3. Check if measurements are writen by loggin in to the influx ui on `http://localhost:8086` using `usernameonlyfortesting` as username and `passwordpasswordonlyfortesting` as password
