FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY prefect/flows/etl_*.py opt/prefect/flows
COPY prefect/data opt/prefect/data
# RUN mkdir -p /prefect/data/yellow