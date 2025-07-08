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


suite("test_complex_types_nested_time", "p0") {

    def table_name = "test_complex_types_nested_time"

    sql """
        DROP TABLE IF EXISTS ${table_name};
    """

    boolean findException = false
    try {
        sql """
            CREATE TABLE ${table_name} (
                id INT,
                array_time ARRAY<TIME>
            ) ENGINE=OLAP 
            DUPLICATE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            )
        """
    } catch (Exception e) {
        logger.error("error: ${e}")
        assertTrue(e.getMessage().contains("ARRAY unsupported sub-type: time(0)"))
        findException = true
    }

    findException = false
    try {
        sql """
            CREATE TABLE ${table_name} (
                id INT,
                map_time MAP<INT, TIME>
            ) ENGINE=OLAP 
            DUPLICATE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            )
        """
    } catch (Exception e) {
        logger.error("error: ${e}")
        assertTrue(e.getMessage().contains("MAP unsupported sub-type: time(0)"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql """
            CREATE TABLE ${table_name} (
                id INT,
                struct_time STRUCT<time: TIME>
            ) ENGINE=OLAP 
            DUPLICATE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            )
        """
    } catch (Exception e) {
        logger.error("error: ${e}")
        assertTrue(e.getMessage().contains("STRUCT unsupported sub-type: time(0)"))
        findException = true
    }
    assertTrue(findException)


    findException = false
    try {
        sql """
           create table test_array_time as select 2, array(timediff('2020-01-01 12:05:03', '2020-01-01 08:02:15'));
        """
    } catch (Exception e) {
        logger.error("error: ${e}")
        assertTrue(e.getMessage().contains("ARRAY unsupported sub-type: time(0)"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql """
           create table test_map_time as select 3, map(1, timediff('2020-01-01 12:05:03', '2020-01-01 08:02:15'));
        """
    } catch (Exception e) {
        logger.error("error: ${e}")
        assertTrue(e.getMessage().contains("MAP unsupported sub-type: time(0)"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql """
           create table test_struct_time as select 4, struct('time', timediff('2020-01-01 12:05:03', '2020-01-01 08:02:15'));
        """
    } catch (Exception e) {
        logger.error("error: ${e}")
        assertTrue(e.getMessage().contains("STRUCT unsupported sub-type: time(0)"))
        findException = true
    }
    assertTrue(findException)
}