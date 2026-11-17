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

# Hive 3.1.3 with krb5 1.18.3-6+deb11u8, built from the pinned recipe in #68440.
# Keep this branch's entrypoint and health checks in the small layer below.
FROM doristhirdpartydocker/hive@sha256:b094fbc1a61c65116fbe2f0bca562687a10af418e7a0b2865f8d7b70cd8ad03e

USER root

COPY entrypoint-hive-master.sh /opt/doris/entrypoint.sh
COPY health-checks/health.sh /opt/doris/health.sh

RUN chmod 755 /opt/doris/entrypoint.sh /opt/doris/health.sh

ENTRYPOINT ["/opt/doris/entrypoint.sh"]
