#!/bin/sh
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

set -eu
mc alias set local http://minio:9000 admin password
mc mb --ignore-existing local/scan-planning
mc anonymous set none local/scan-planning
mc admin user add local scan_data_reader ScanDataOnly2026
mc admin policy create local scan-data-only /fixtures/data-only-policy.json
mc admin policy attach local scan-data-only --user scan_data_reader
