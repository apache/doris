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

suite("test_basic_statistics") {
    String db = "test_basic_statistics"
    String tbl = "test_table_1"
    sql """set global force_sample_analyze=false"""

    sql """
        DROP DATABASE IF EXISTS `${db}`
    """

    sql """
        CREATE DATABASE `${db}`
    """

    sql """ use `${db}`"""

    sql """
        DROP TABLE IF EXISTS `${tbl}`
    """

    sql """
          CREATE TABLE IF NOT EXISTS `${tbl}` (
            `id` int(11) not null comment "",
            `name` varchar(100) null comment ""
        ) engine=olap
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1 properties("replication_num" = "1")
    """

    sql """
        INSERT INTO `${tbl}` VALUES (1, 'name1'), (2, 'name2'), (3, 'name3'), (4, 'name4'), (5, 'name5'), (6, 'name6'), (7, 'name7'), (8, 'name8'), (9, 'name9')
    """

    sql """ analyze table ${tbl} with sync"""
    def result = sql """show column stats ${tbl} (id)"""
    assertEquals(result.size(), 1)
    assertEquals(result[0][0], "id")
    assertEquals(result[0][2], "9.0")
    assertEquals(result[0][3], "9.0")
    assertEquals(result[0][4], "0.0")
    assertEquals(result[0][5], "36.0")
    assertEquals(result[0][6], "4.0")
    assertEquals(result[0][7], "1")
    assertEquals(result[0][8], "9")

    result = sql """show column stats ${tbl} (name)"""
    assertEquals(result.size(), 1)
    assertEquals(result[0][0], "name")
    assertEquals(result[0][2], "9.0")
    assertEquals(result[0][3], "9.0")
    assertEquals(result[0][4], "0.0")
    assertEquals(result[0][5], "45.0")
    assertEquals(result[0][6], "5.0")
    assertEquals(result[0][7], "\'name1\'")
    assertEquals(result[0][8], "\'name9\'")

    sql """drop stats ${tbl}"""
    sql """drop table ${tbl}"""
    sql """drop database ${db}"""

}
