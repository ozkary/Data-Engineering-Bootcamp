
# Build the image using the default Dockerfile

$ docker build -t taxi_ingest:v001 .

## Run with a custom network name
$ docker run -it --network=2_docker_sql_default taxi_ingest:v001...