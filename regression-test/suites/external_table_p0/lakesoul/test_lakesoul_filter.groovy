// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_lakesoul_filter", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableLakesoulTest")
    // open it when docker image is ready to run in regression test
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "lakesoul"
        String db_name = "default"
        String pg_user = context.config.otherConfigs.get("lakesoulPGUser")
        String pg_pwd = context.config.otherConfigs.get("lakesoulPGPwd")
        String pg_url = context.config.otherConfigs.get("lakesoulPGUrl")
        String minio_ak = context.config.otherConfigs.get("lakesoulMinioAK")
        String minio_sk = context.config.otherConfigs.get("lakesoulMinioSK")
        String minio_endpoint = context.config.otherConfigs.get("lakesoulMinioEndpoint")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog lakesoul  properties (
            'type'='lakesoul',
            'lakesoul.pg.username'='${pg_user}',
            'lakesoul.pg.password'='${pg_pwd}',
            'lakesoul.pg.url'='${pg_url}',
            'minio.endpoint'='${minio_endpoint}',
            'minio.access_key'='${minio_ak}',
            'minio.secret_key'='${minio_sk}'
            );"""

        // analyze
        sql """use `${catalog_name}`.`${db_name}`"""

        sql """show tables;"""
        // select
        sql """select * from region;"""

        sql """select * from nation;"""

        sql """select * from nation where n_regionkey = 0 or n_nationkey > 14;"""

        sql """select * from nation where n_regionkey = 0 and n_nationkey > 0;"""

        sql """select * from nation where n_regionkey = 0;"""

        // non-selecting query
        sql """select count(*) from customer;"""

        // filter by non-partition column
        sql """select count(*) from customer where c_mktsegment='BUILDING';"""

        // filter by partition column
        sql """select count(*) from customer where c_nationkey=19;"""

        // filter by both partition and non-partition column
        sql """select count(*) from customer where c_mktsegment='BUILDING' and c_nationkey=19;"""

        sql """select * from lineitem where l_shipdate <= DATE '1992-12-01' limit 10;"""

        sql """select count(*) from part where p_type like 'MEDIUM POLISHED%';"""
    }
}

