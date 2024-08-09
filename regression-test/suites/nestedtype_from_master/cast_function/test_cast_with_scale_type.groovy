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

suite("test_cast_with_scale_type") {

    def tableName = "test_cast_with_scale_type_table"
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE IF NOT EXISTS `${tableName}` (
            `uid` int(11) NULL COMMENT "",
            `c_datetimev2` datetimev2(3) NULL COMMENT "",
            `c_string` varchar(30) NULL COMMENT ""
            ) ENGINE=OLAP
        DUPLICATE KEY(`uid`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`uid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        )
        """

        sql """INSERT INTO ${tableName} values
        (1,"2022-12-01 22:23:24.999999",'2022-12-01 22:23:24.999999'),
        (2,"2022-12-02 22:23:24.999999",'2022-12-02 22:23:24.999999')
        """
        
        qt_select1 "select * from ${tableName}"
        qt_select2 "select uid, c_datetimev2, cast(c_string as datetimev2(3)) from ${tableName} order by uid asc"
        qt_select3 "select cast ('2022-12-02 22:23:24.999999' as datetimev2(3)),cast ('2022-12-02 22:23:23.999999' as datetimev2(3)) from ${tableName}"
        qt_select4 "select cast ('2022-12-02 22:23:24.999999' as datetimev2(3)),cast ('2022-12-02 22:23:23.999999' as datetimev2(3))"

        sql "DROP TABLE IF EXISTS ${tableName}"
}