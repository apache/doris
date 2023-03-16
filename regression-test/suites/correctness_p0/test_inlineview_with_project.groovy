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

suite("test_inlineview_with_project") {
    sql """
        drop table if exists cir_1756_t1;
    """

    sql """
        drop table if exists cir_1756_t2;
    """
    
    sql """
        create table cir_1756_t1 (`date` date not null)
        ENGINE=OLAP
        DISTRIBUTED BY HASH(`date`) BUCKETS 5
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table cir_1756_t2 ( `date` date not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(`date`) BUCKETS 5
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into cir_1756_t1 values("2020-02-02");
    """

    sql """
        insert into cir_1756_t2 values("2020-02-02");
    """

    qt_select """
        WITH t0 AS(
            SELECT report.date1 AS date2 FROM(
                SELECT DATE_FORMAT(date, '%Y%m%d') AS date1 FROM cir_1756_t1
            ) report GROUP BY report.date1
            ),
            t3 AS(
                SELECT date_format(date, '%Y%m%d') AS `date3`
                FROM `cir_1756_t2`
            )
        SELECT row_number() OVER(ORDER BY date2)
        FROM(
            SELECT t0.date2 FROM t0 LEFT JOIN t3 ON t0.date2 = t3.date3
        ) tx;
    """

    sql """
        drop table if exists cir_1756_t1;
    """

    sql """
        drop table if exists cir_1756_t2;
    """
}
