# Apache Superset image for the BI layer (DPFMD-40).
#
# The official image ships Superset's Trino engine spec but not the DBAPI /
# SQLAlchemy dialect, so the "Trino Iceberg" database connection would fail to
# load. The pin matches the Great Expectations project, which already validates
# this dialect against the same Trino 476 server.
FROM apache/superset:4.1.2

USER root

RUN pip install --no-cache-dir "trino[sqlalchemy]==0.330.0"

# Superset imports superset_config.py from PYTHONPATH; the base image already
# sets PYTHONPATH=/app/pythonpath. The bootstrap script is run by the
# Deployment's initContainer after `superset db upgrade` / `superset init`.
COPY superset_config.py /app/pythonpath/superset_config.py
COPY bootstrap_superset.py /app/bootstrap_superset.py

USER superset
