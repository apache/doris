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

suite("test_lakesoul_catalog", "p0,external,doris,external_docker,external_docker_doris") {
    def enabled = false;
    // open it when docker image is ready to run in regression test
    if (enabled) {
        String catalog_name = "lakesoul"
        String db_name = "default"

        sql """drop catalog if exists ${catalog_name}"""
        sql """
            create catalog lakesoul  properties ('type'='lakesoul','lakesoul.pg.username'='lakesoul_test','lakesoul.pg.password'='lakesoul_test','lakesoul.pg.url'='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified');"""

        // analyze
        sql """use `${catalog_name}`.`${db_name}`"""

 sq     """show tables;"""
        // select
        sql  """select * from nation;"""

        sql  """show create table nation;"""
    }
}

