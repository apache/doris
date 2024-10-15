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
suite("test_alter_table_property") {
    def tableName = "test_table"
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
            CREATE TABLE ${tableName} (
                    `c_custkey` int(11) NOT NULL COMMENT "",
                    `c_name` varchar(26) NOT NULL COMMENT "",
                    `c_address` varchar(41) NOT NULL COMMENT "",
                    `c_city` varchar(11) NOT NULL COMMENT ""
            )
            DUPLICATE KEY (`c_custkey`)
            DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
            PROPERTIES (
                    "replication_num" = "1"
             );
        """
    sql """sync"""

    def showResult1 = sql """show create table ${tableName}"""
    logger.info("${showResult1}")
    assertTrue(showResult1.toString().containsIgnoreCase('"enable_single_replica_compaction" = "false"'))

    if (!isCloudMode()) {
        sql """
            alter table ${tableName} set ("enable_single_replica_compaction" = "true")
            """
        sql """sync"""

        def showResult2 = sql """show create table ${tableName}"""
        logger.info("${showResult2}")
        assertTrue(showResult2.toString().containsIgnoreCase('"enable_single_replica_compaction" = "true"'))
    }

    assertTrue(showResult1.toString().containsIgnoreCase('"disable_auto_compaction" = "false"'))

    sql """
        alter table ${tableName} set ("disable_auto_compaction" = "true")
        """
    sql """sync"""
    def showResult3 = sql """show create table ${tableName}"""
    logger.info("${showResult3}")
    assertTrue(showResult3.toString().containsIgnoreCase('"disable_auto_compaction" = "true"'))

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """sync"""
}
