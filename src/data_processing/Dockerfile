# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Image for the Bronze Parquet writer (DPFMD-34) so it can actually run as a
# Deployment. DPFMD-34 delivered the package but no image, which is why nothing
# was consuming the orders topic -- see DPFMD-46.

FROM python:3.12-slim

# requirements.txt is compiled with uv for CPython 3.12 and pins pyarrow, so the
# install resolves to prebuilt wheels. Building pyarrow from source on arm64
# would need a full toolchain for no benefit.
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --only-binary=:all: -r requirements.txt

# Only the writer package. seed_database.py and make_bucket_public.py are
# one-shot helpers that run on the host, and tests/ is not needed at runtime.
COPY lakehouse ./lakehouse
COPY bronze_orders_writer.py ./

# The base image runs as root; this service only needs outbound network and a
# writable /tmp, so drop privileges rather than running the consumer as root.
RUN useradd --create-home --uid 10001 appuser \
 && chown -R appuser:appuser /app
USER 10001

# `python -m lakehouse.runner` is the entrypoint because kafka-python's
# consumer needs the package importable from WORKDIR.
CMD ["python", "-m", "lakehouse.runner"]