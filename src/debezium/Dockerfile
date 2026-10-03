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

# The quay.io/debezium/connect image ships no Avro converter. The only Avro
# jars it contains are the Apicurio serde under /kafka/external_libs/apicurio,
# and those hold no Kafka Connect Converter class. Debezium's Avro
# documentation therefore requires the Confluent Avro converter to be added to
# the connector's plugin directory, which is what this image does.

# Define a default value so it's not empty if the builder fails to provide it
ARG BUILDPLATFORM=linux/amd64

# Resolve the dependency closure on the build platform so this stage does not
# run under emulation when cross-building for another architecture.
FROM --platform=$BUILDPLATFORM maven:3.9-eclipse-temurin-21@sha256:99e61abcff91a9b1333463bd8451fb18495d6eba9250ac66a338b518f8278320 AS deps

WORKDIR /build

COPY pom.xml .

# The pom.xml exclusions, not this flag, are what keep kafka-clients and
# slf4j-api out: both arrive as compile-scope transitives, and a second copy of
# either inside a plugin directory lands on the plugin classloader.
RUN mvn -B -ntp dependency:copy-dependencies \
      -DincludeScope=runtime \
      -DoutputDirectory=/out

# No --platform here on purpose. The worker has to match the node
# architecture of the cluster it connects to, so this stage must be built for
# the target platform rather than the build platform.
FROM quay.io/debezium/connect:2.7.3.Final@sha256:a51bbb3a08d0de2cd56f76b956060bdaa3beb112ec13dac2668886b92dd5dd0b

# The base image already runs as the unprivileged "kafka" user. Restated with
# the name rather than a numeric uid so the container keeps working if that
# uid ever changes upstream, and so scanners that only look for a literal USER
# instruction can see that this image is not running as root.
USER kafka

# Kafka Connect treats every subdirectory of plugin.path as an isolated plugin,
# so the converter gets its own directory rather than being merged into
# debezium-connector-postgres the way the Debezium docs describe.
# COPY keeps 0:0 ownership, and the jars land world-readable.
COPY --from=deps /out/ /kafka/connect/avro-converter/
