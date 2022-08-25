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

suite("test_ctas") {
    sql """
    CREATE TABLE IF NOT EXISTS `test_ctas` (
      `test_varchar` varchar(150) NULL,
      `test_datetime` datetime NULL,
      `test_default_timestamp` datetime DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=OLAP
    UNIQUE KEY(`test_varchar`)
    DISTRIBUTED BY HASH(`test_varchar`) BUCKETS 3
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    )
    """

    sql """ INSERT INTO test_ctas(test_varchar, test_datetime) VALUES ('test1','2022-04-27 16:00:33'),('test2','2022-04-27 16:00:54') """

    sql """ 
    CREATE TABLE IF NOT EXISTS `test_ctas1` 
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    ) as select * from test_ctas;
    """

    qt_select """SHOW CREATE TABLE `test_ctas1`"""

    qt_select """select count(*) from test_ctas1"""

    sql """ DROP TABLE IF EXISTS test_ctas """

    sql """ DROP TABLE IF EXISTS test_ctas1 """

}
