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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("test_user_activity") {
    sql """set enable_nereids_planner=true"""
    sql """ DROP TABLE IF EXISTS u_axx; """

    sql """
            CREATE TABLE u_axx (
                r_xx INT,
                n_dx DATE,
                n_duration INT
            )
            DISTRIBUTED BY HASH(r_xx)
            PROPERTIES (
                "replication_num" = "1"
            );
        """

    sql """INSERT INTO u_axx VALUES (1, "2023-01-02", 300);"""
    sql """INSERT INTO u_axx VALUES (2, "2023-01-02", 600);"""

    qt_select_base " select n_dx, percentile_approx(n_duration, 0.5) as p50, percentile_approx(n_duration, 0.90) as p90 FROM u_axx GROUP BY n_dx; "

    sql """ drop materialized view IF EXISTS session_distribution_2 on u_axx;"""
    createMV ("create materialized view session_distribution_2 as select n_dx, percentile_approx(n_duration, 0.5) as p50, percentile_approx(n_duration, 0.90) as p90 FROM u_axx GROUP BY n_dx;")

    sql """INSERT INTO u_axx VALUES (2, "2023-01-02", 600);"""

    qt_select_star "select * from u_axx order by 1;"

    sql """analyze table u_axx with sync;"""
    sql """set enable_stats=false;"""

    mv_rewrite_success("select n_dx, percentile_approx(n_duration, 0.5) as p50, percentile_approx(n_duration, 0.90) as p90 FROM u_axx GROUP BY n_dx;",
            "session_distribution_2")
    qt_select_group_mv "select n_dx, percentile_approx(n_duration, 0.5) as p50, percentile_approx(n_duration, 0.90) as p90 FROM u_axx GROUP BY n_dx;"

    sql """set enable_stats=true;"""
    sql """alter table u_axx modify column r_xx set stats ('row_count'='3');"""
    mv_rewrite_success("select n_dx, percentile_approx(n_duration, 0.5) as p50, percentile_approx(n_duration, 0.90) as p90 FROM u_axx GROUP BY n_dx;",
            "session_distribution_2")
}
