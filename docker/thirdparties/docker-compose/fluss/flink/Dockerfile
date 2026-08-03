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
# assembles both directories, lib/ from a local fluss checkout (fluss 1.0 is not
# released yet) and paimon from the maven repository.
ARG FLINK_BASE_IMAGE=flink:1.20.0-scala_2.12-java17
FROM ${FLINK_BASE_IMAGE}

COPY lib/ /opt/flink/lib/

# The tiering job is submitted with `flink run`, so it is a job artifact rather
# than a cluster dependency: it belongs in opt/, not lib/. Upstream's quickstart
# image puts it in the same place.
COPY opt/ /opt/flink/opt/
