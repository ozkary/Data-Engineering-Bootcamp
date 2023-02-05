
# Build the image using the default Dockerfile

$ docker build -t taxi_ingest:v001 .

## Run with a custom network name
docker run -it --network=docker_default taxi_ingest:v001 --user=root --password=root --host=pgdatabase --port=5432 --db=ny_taxi --table_name=yellow_taxi_trips --url=${URL}