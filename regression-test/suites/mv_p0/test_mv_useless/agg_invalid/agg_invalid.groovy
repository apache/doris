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

suite ("agg_invalid") {
    sql """drop table if exists t1;"""

    sql """
        CREATE TABLE t1 (   p1 INT,   p2 INT,   p3 INT,   v1 INT SUM,   v2 INT MAX,   v3 INT MIN ) AGGREGATE KEY (p1, p2, p3) DISTRIBUTED BY HASH (p1) BUCKETS 1 PROPERTIES ('replication_num' = '1');
        """

    test {
        sql "CREATE MATERIALIZED VIEW mv_1 AS SELECT p1, SUM(v3) FROM t1 GROUP BY p1;"
        exception "errCode = 2,"
    }

    test {
        sql "CREATE MATERIALIZED VIEW mv_2 AS SELECT p1, MIN(v3+v3) FROM t1 GROUP BY p1;"
        exception "errCode = 2,"
    }

    test {
        sql "CREATE MATERIALIZED VIEW mv_3 AS SELECT p1, SUM(v1) FROM t1 GROUP BY p1;"
        exception null
    }

    test {
        sql "CREATE MATERIALIZED VIEW mv_4 AS SELECT p1, SUM(abs(v1)) FROM t1 GROUP BY p1;"
        exception "errCode = 2,"
    }

    test {
        sql "CREATE MATERIALIZED VIEW mv_5 AS SELECT p1, p2, p1, SUM(v1) FROM t1 GROUP BY p1,p2,p1;"
        exception "errCode = 2,"
    }

    test {
        sql "CREATE MATERIALIZED VIEW mv_5 AS SELECT p1, p2, SUM(v1), SUM(v1) FROM t1 GROUP BY p1,p2;"
        exception "errCode = 2,"
    }
}
