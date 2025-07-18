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

version: '3'

services:
  openldap:
    image: osixia/openldap:1.5.0
    container_name: doris--openldap
    hostname: ${LDAP_HOST_NAME}
    ports:
      - "${LDAP_HOST_PORT}:389"
      - "${LDAPS_HOST_PORT}:636"
    volumes:
      - ./data:/var/lib/ldap
      - ./slapd.d:/etc/ldap/slapd.d
    environment:
      - LDAP_ORGANISATION=${LDAP_ORGANISATION}
      - LDAP_DOMAIN=${LDAP_DOMAIN}
      - LDAP_ADMIN_PASSWORD=${LDAP_ADMIN_PASSWORD}
      - LDAP_CONFIG_PASSWORD=${LDAP_CONFIG_PASSWORD}
    networks:
      - ldap-net

  phpldapadmin:
    image: osixia/phpldapadmin:0.9.0
    container_name: doris--phpldapadmin
    hostname: ${PHPLDAPADMIN_HOST_NAME}
    ports:
      - "${PHPLDAPADMIN_HOST_PORT}:80"
    environment:
      - PHPLDAPADMIN_LDAP_HOSTS=${PHPLDAPADMIN_LDAP_HOSTS}
      - PHPLDAPADMIN_HTTPS=false
    depends_on:
      - openldap
    networks:
      - ldap-net

networks:
  ldap-net:
    driver: bridge