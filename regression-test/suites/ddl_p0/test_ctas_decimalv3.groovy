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

suite("test_ctas_decimalv3") {
    def table1 = "test_uniq_tab_decimalv3"
    def table2 = "test_uniq_tab_decimalv3_2"
    try {
        sql """
    CREATE TABLE ${table1} (
    c1 DECIMALV3(7,1) NULL,
    c2 DECIMALV3(15,5) NULL,
    c3 DECIMALV3(30,1) NULL,
    v1 DECIMALV3(7,6) NULL COMMENT "",
    v2 DECIMALV3(18,6) NULL COMMENT "",
    v3 DECIMALV3(30,6) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(c1,c2,c3)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(c1,c2,c3) BUCKETS 3
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    )
    """

      sql """ insert into ${table1} values (1.1,1.2,1.3,1.4,1.5,1.6) """
      sql """ insert into ${table1} values (2.1,2.2,2.3,2.4,2.5,2.6) """
      sql """ insert into ${table1} values (1.1,1.2,1.3,1.4,1.5,1.6) """
      sql """ insert into ${table1} values (2.1,2.2,2.3,2.4,2.5,2.6) """

      qt_select """select * from ${table1} order by c1;"""
      sql """
    CREATE TABLE IF NOT EXISTS `${table2}`
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    ) as select * from ${table1};
    """

      qt_select """select * from ${table2} order by c1;"""


    } finally {
        sql """ DROP TABLE IF EXISTS ${table1} """

        sql """ DROP TABLE IF EXISTS ${table2} """
    }

}

