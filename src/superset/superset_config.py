"""Superset configuration for the local dev stack (DPFMD-40)."""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_METADATA_URI"]

# The dev stack serves plain HTTP on a trusted local network. Talisman and
# secure cookies would redirect requests to HTTPS or drop the session cookie.
TALISMAN_ENABLED = False
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_SAMESITE = "Lax"

# Single-node deployment: no Redis/Celery stack, SQL Lab runs in the web
# process, and the dataset is small enough that public rate limits get in the
# way of loading a dashboard.
RATELIMIT_ENABLED = False
SQLLAB_ASYNC_TIME_LIMIT_SEC = 120
