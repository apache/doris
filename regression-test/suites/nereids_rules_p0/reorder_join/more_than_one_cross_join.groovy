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

suite("more_than_one_cross_join") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """
        DROP TABLE IF EXISTS more_than_one_cross_join1
    """
    sql """
        DROP TABLE IF EXISTS more_than_one_cross_join2
    """

    sql """
        CREATE TABLE `more_than_one_cross_join1` (
          `pk` INT NULL,
          `cv10` VARCHAR(10) NULL,
          `ci` INT NULL,
          `cv1024` VARCHAR(1024) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`pk`, `cv10`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`pk`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        CREATE TABLE `more_than_one_cross_join2` (
          `pk` INT NULL,
          `cv10` VARCHAR(10) NULL,
          `ci` INT NULL,
          `cv1024` VARCHAR(1024) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`pk`, `cv10`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`pk`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        SELECT
          alias1.pk AS field1, 
          alias1.pk AS field2,
          alias2.ci AS field3
        FROM
          more_than_one_cross_join1 AS alias1, 
          more_than_one_cross_join1 AS alias2, 
          more_than_one_cross_join2 AS alias3 
        WHERE
          alias1.pk != alias3.ci
        GROUP BY
          field1, field2, field3
        HAVING
          (((field3 < 1 AND field1 = 3) AND field2 = 5) OR field3 > 0)
        ORDER BY
          field1, field2, field3
    """
}
