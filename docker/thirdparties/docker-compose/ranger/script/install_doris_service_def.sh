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

curl -O https://s3BucketName.s3Endpoint/regression/docker/ranger-plugins/ranger-servicedef-doris.json
until curl -f http://localhost:6080; do
    echo "Waiting for service to be healthy..."
    sleep 30
done
curl -u admin:Ranger1234 -X POST \
    -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    http://localhost:6080/service/plugins/definitions \
    -d@ranger-servicedef-doris.json