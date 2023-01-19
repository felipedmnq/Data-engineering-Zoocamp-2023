FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

ARG password
ENV env_var_name=$password
WORKDIR /app
COPY Data-engineering-Zoocamp-2023/scripts/ingest_data.py ingest_data.py

ENTRYPOINT ["python3", "ingest_data.py"]