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

suite ("sql_default_limit") {

    // this mv rewrite would not be rewritten in RBO phase, so set TRY_IN_RBO explicitly to make case stable
    sql "set pre_materialized_view_rewrite_strategy = TRY_IN_RBO"
    String db = context.config.getDbNameByFile(context.file)
    sql """use ${db}"""

    sql """ DROP TABLE IF EXISTS sql_default_limit_table; """

    sql """
        create table sql_default_limit_table
        (
        id1 int,
        id2 int,
        id3 int,
        sale_date date,
        sale_amt bigint
        )
        distributed by hash(id1)
        properties("replication_num" = "1");
        """

    sql """insert into sql_default_limit_table values(1,1,1,'2020-02-02',1);"""
    sql """insert into sql_default_limit_table values(1,1,1,'2020-02-02',1);"""
    sql """insert into sql_default_limit_table values(1,1,1,'2020-02-02',1);"""
    sql """insert into sql_default_limit_table values(1,1,1,'2020-02-02',1);"""
    sql """insert into sql_default_limit_table values(2,1,1,'2020-02-02',1);"""
    sql """insert into sql_default_limit_table values(1,1,1,'2020-02-02',1);"""

    create_sync_mv(db, "sql_default_limit_table", "test_mv",
            """select id1 as a1, sum(sale_amt) from sql_default_limit_table group by id1""")

    sql """analyze table sql_default_limit_table with sync;"""
    sql """alter table sql_default_limit_table modify column sale_amt set stats ('row_count'='6');"""


    sql """set enable_stats=true;"""
    sql """set sql_select_limit = 1;"""
    mv_rewrite_success("select id1, sum(sale_amt) from sql_default_limit_table group by id1 order by id1;",
            "test_mv")
    order_qt_query1 """select id1, sum(sale_amt) from sql_default_limit_table group by id1;"""
    sql """set sql_select_limit = -1;"""

    sql """set default_order_by_limit = 2;"""
    mv_rewrite_success("select id1, sum(sale_amt) from sql_default_limit_table group by id1 order by id1;",
            "test_mv")
    order_qt_query2 """select id1, sum(sale_amt) from sql_default_limit_table group by id1;"""
}
