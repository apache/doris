#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Flink image used to prepare the fluss regression data: stock Flink plus the
# fluss connector, the paimon runtime and the lake tiering job. build-images.sh
# assembles both directories from maven repositories: the fluss jars from
# FLUSS_MAVEN_REPO_URL in fluss.env.tpl (the staging repository of the 1.0.0
# release candidate until the release reaches central), paimon and hadoop from
# central. The default base is the Flink the pinned connector is built
# against; build-images.sh passes FLINK_BASE_IMAGE explicitly anyway.
ARG FLINK_BASE_IMAGE=flink:1.20.3-scala_2.12-java17
FROM ${FLINK_BASE_IMAGE}

COPY lib/ /opt/flink/lib/

# The tiering job is submitted with `flink run`, so it is a job artifact rather
# than a cluster dependency: it belongs in opt/, not lib/. Upstream's quickstart
# image puts it in the same place.
COPY opt/ /opt/flink/opt/
