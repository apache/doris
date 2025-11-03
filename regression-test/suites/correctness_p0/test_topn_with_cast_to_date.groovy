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

suite("test_topn_with_cast_to_date") {

    sql """
        DROP TABLE IF EXISTS t_test_topn_with_cast_to_date 
    """

    sql """
        create table t_test_topn_with_cast_to_date(k1 datetimev2, k2 int) distributed by hash(k1) buckets 1 properties("replication_num" = "1");
    """

    sql """
        insert into t_test_topn_with_cast_to_date values("2016-11-04 00:00:01", 1);
    """

    qt_sql """
        SELECT dt
        FROM 
            (SELECT cast(k1 AS datev2) AS dt
            FROM t_test_topn_with_cast_to_date ) r
        GROUP BY  dt
        ORDER BY  dt;
    """
}
