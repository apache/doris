#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

{
    while true; do
        if mc ping --count 1 local; then
            echo "minio server is reachable."
            mc config host add local http://localhost:9000 minioadmin minioadmin
            mc mb -p local/test-bucket
            break
        else
            echo "minio server is not reachable. Retrying in 1 seconds..."
            sleep 1
        fi
    done
} &

/bin/docker-entrypoint.sh server /data --console-address ':9001'
