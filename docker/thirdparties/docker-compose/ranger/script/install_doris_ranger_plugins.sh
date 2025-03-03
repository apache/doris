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

#!/bin/bash
set -ex

if [ ! -d "${RANGER_HOME}/ews/webapp/WEB-INF/classes/ranger-plugins/doris" ]; then
    mkdir -p "${RANGER_HOME}/ews/webapp/WEB-INF/classes/ranger-plugins/doris"
fi
cd "${RANGER_HOME}/ews/webapp/WEB-INF/classes/ranger-plugins/doris"
curl -O https://s3BucketName.s3Endpoint/regression/docker/ranger-plugins/mysql-connector-java-8.0.25.jar
curl -O https://s3BucketName.s3Endpoint/regression/docker/ranger-plugins/ranger-doris-plugin-3.0.0-SNAPSHOT.jar