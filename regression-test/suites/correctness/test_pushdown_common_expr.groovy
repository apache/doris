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

suite("test_pushdown_common_expr") {
    sql """ DROP TABLE IF EXISTS t_pushdown_common_expr """
    sql """
    CREATE TABLE `t_pushdown_common_expr` (
        `c1` int(11) NULL,
        `c2` varchar(100) NULL COMMENT "",
        `c3` varchar(100) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`c1`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`c1`) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
        INSERT INTO t_pushdown_common_expr VALUES
        (1,'a','aa'),
        (2,'b','bb'),
        (4,'c','cc'),
        (8,'d','dd'),
        (16,'e','ee'),
        (32,'f','ff'),
        (64,'g','gg'),
        (128,'h','hh'),
        (256,'i','ii'),
        (512,'j','jj'),
        (1024,'k','kk');
     """

    sql """set batch_size=8"""
    sql """set enable_common_expr_pushdown=true"""

    order_qt_1 """
        SELECT * FROM t_pushdown_common_expr WHERE c3 LIKE "%c%" OR c1 < 10;
    """

    order_qt_2 """
        SELECT * FROM t_pushdown_common_expr WHERE UPPER(c2)="G" OR UPPER(c2)="P";
    """

    order_qt_3 """
        SELECT * FROM t_pushdown_common_expr WHERE c1 = 256 OR c1 = 100;
    """

    order_qt_4 """
        SELECT * FROM t_pushdown_common_expr WHERE c1 < 300 OR UPPER(c2)="F" OR c3 LIKE "%f%";
    """

    sql """set enable_common_expr_pushdown=false"""

    order_qt_1 """
        SELECT * FROM t_pushdown_common_expr WHERE c3 LIKE "%h%" OR c1 < 10;
    """

    order_qt_2 """
        SELECT * FROM t_pushdown_common_expr WHERE UPPER(c2)="G" OR UPPER(c2)="P";
    """

    order_qt_3 """
        SELECT * FROM t_pushdown_common_expr WHERE c1 = 256 OR c1 = 100;
    """

    order_qt_4 """
        SELECT * FROM t_pushdown_common_expr WHERE c1 < 300 OR UPPER(c2)="K" OR c3 LIKE "%k%";
    """

}
