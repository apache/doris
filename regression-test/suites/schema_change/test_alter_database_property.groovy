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

suite("test_alter_database_property") {
    def ret = sql "SHOW FRONTEND CONFIG like '%enable_feature_binlog%';"
    logger.info("${ret}")
    if (ret.size() != 0 && ret[0].size() > 1 && ret[0][1] == 'false') {
        logger.info("enable_feature_binlog=false in frontend config, no need to run this case.")
        return
    }

    sql "drop database if exists test_alter_database_property"

    sql """
        create database test_alter_database_property
        """
    def result = sql "show create database test_alter_database_property"
    logger.info("${result}")

    // Case 1: alter database, set binlog enable is true
    sql """
        alter database test_alter_database_property set properties ("binlog.enable" = "true")
        """
    result = sql "show create database test_alter_database_property"
    logger.info("${result}")
    assertTrue(result.toString().containsIgnoreCase('"binlog.enable" = "true"'))

    // Case 2:
    // create table, table binlog enable is false, so database binlog enable setting true not success
    sql """
        alter database test_alter_database_property set properties ("binlog.enable" = "false")
        """
    result = sql "show create database test_alter_database_property"
    logger.info("${result}")
    assertTrue(result.toString().containsIgnoreCase('"binlog.enable" = "false"'))
    // create table t1 binlog disable
    sql """
        CREATE TABLE test_alter_database_property.t1
        (
            k1 INT
        )
        ENGINE = olap
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        )
    """
    // check enable db binlog error
    assertThrows(Exception.class, {
        sql """
            alter database test_alter_database_property set properties ("binlog.enable" = "true")
            """
    })
    // set table t1 binlog true
    sql """
        alter table test_alter_database_property.t1 set ("binlog.enable" = "true");
    """
    // check db enable binlog true
    sql """
        alter database test_alter_database_property set properties ("binlog.enable" = "true")
        """
    result = sql "show create database test_alter_database_property"
    logger.info("${result}")
    assertTrue(result.toString().containsIgnoreCase('"binlog.enable" = "true"'))

    // Case 3: table false, db can set binlog.enable = false
    sql """
        CREATE TABLE test_alter_database_property.t2
        (
            k1 INT
        )
        ENGINE = olap
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        )
    """
    sql """
        alter database test_alter_database_property set properties ("binlog.enable" = "false")
        """
    result = sql "show create database test_alter_database_property"
    logger.info("${result}")
    assertTrue(result.toString().containsIgnoreCase('"binlog.enable" = "false"'))

    sql "drop database if exists test_alter_database_property"
}
