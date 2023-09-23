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

suite("test_implicit_convert") {

    sql """
        DROP TABLE IF EXISTS part_by_date
    """
    sql """
        CREATE TABLE `part_by_date`
        (
            `date`                       date         NOT NULL COMMENT '',
            `id`                      int(11) NOT NULL COMMENT ''
        ) ENGINE=OLAP
        UNIQUE KEY(`date`, `id`)
        PARTITION BY RANGE(`date`) 
        (PARTITION p201912 VALUES [('0000-01-01'), ('2020-01-01')),
        PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')))
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO  part_by_date VALUES('0001-02-01', 1),('2020-01-15', 2);
    """

    qt_sql """
        SELECT
            id
        FROM
           part_by_date
        WHERE date > 20200101;
    """
}