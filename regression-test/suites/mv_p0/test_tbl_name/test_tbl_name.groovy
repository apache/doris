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

suite ("test_tbl_name") {

    sql """drop table if exists functionality_olap;"""

    sql """
             create table functionality_olap(
            id int,
            type varchar(20),
            score  int
            )DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """

    sql """insert into functionality_olap values(141,'mv',18);"""

    createMV ("""CREATE MATERIALIZED VIEW MV_OLAP_SUM as select functionality_olap.id as id, sum(functionality_olap.score) as score_max from functionality_olap group by functionality_olap.id;""")

    sql """insert into functionality_olap values(143,'mv',18);"""

    sql """analyze table functionality_olap with sync;"""
    sql """set enable_stats=false;"""

    explain {
        sql("""select 
            functionality_olap.id as id,
            sum(functionality_olap.score) as score_max
            from functionality_olap
            group by functionality_olap.id order by 1,2; """)
        contains "(MV_OLAP_SUM)"
    }
    qt_select_mv """select 
            functionality_olap.id as id,
            sum(functionality_olap.score) as score_max
            from functionality_olap
            group by functionality_olap.id order by 1,2;"""

    explain {
        sql("""select 
            id,
            sum(score) as score_max
            from functionality_olap
            group by id order by 1,2;
            """)
        contains "(MV_OLAP_SUM)"
    }
    qt_select_mv """select 
        id,
        sum(score) as score_max
        from functionality_olap
        group by id order by 1,2;
        """
    sql """set enable_stats=true;"""
    explain {
        sql("""select 
            functionality_olap.id as id,
            sum(functionality_olap.score) as score_max
            from functionality_olap
            group by functionality_olap.id order by 1,2; """)
        contains "(MV_OLAP_SUM)"
    }

    explain {
        sql("""select 
            id,
            sum(score) as score_max
            from functionality_olap
            group by id order by 1,2;
            """)
        contains "(MV_OLAP_SUM)"
    }
}
