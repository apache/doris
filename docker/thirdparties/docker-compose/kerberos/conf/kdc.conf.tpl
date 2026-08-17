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

[kdcdefaults]
 kdc_ports = ${KDC_PORT}
 kdc_tcp_ports = ${KDC_PORT}

[realms]
 ${REALM} = {
  database_name = /data/kdc/principal
  key_stash_file = /data/kdc/.k5.${REALM}
  max_life = 24h
  max_renewable_life = 7d
  supported_enctypes = aes128-cts-hmac-sha1-96:normal
 }
