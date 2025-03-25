#!/usr/bin/env bash
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
 kdc_ports = ${KDC_PORT1}
 kdc_tcp_ports = ${KDC_PORT1}
 kadmind_port = ${KADMIND_PORT1}
 kpasswd_port = ${KPASSWD_PORT1}


[realms]
 OTHERREALM.COM = {
  acl_file = /var/kerberos/krb5kdc/kadm5.acl
  dict_file = /usr/share/dict/words
  admin_keytab = /var/kerberos/krb5kdc/kadm5.keytab
  supported_enctypes = aes128-cts:normal des3-hmac-sha1:normal arcfour-hmac:normal des-hmac-sha1:normal des-cbc-md5:normal des-cbc-crc:normal
  kdc_listen = ${KDC_PORT1}
  kdc_tcp_listen = ${KDC_PORT1}
  kdc_ports = ${KDC_PORT1}
  kdc_tcp_ports = ${KDC_PORT1}
  kadmind_port = ${KADMIND_PORT1}
  kpasswd_port = ${KPASSWD_PORT1}
 }