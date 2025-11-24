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

suite ("query_in_different_db") {

    // this mv rewrite would not be rewritten in RBO phase, so set TRY_IN_RBO explicitly to make case stable
    sql "set pre_materialized_view_rewrite_strategy = TRY_IN_RBO"

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql """ DROP TABLE IF EXISTS d_table; """
    sql """
            create table d_table(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 2,2,2,'b';"
    sql "insert into d_table select 2,2,2,'b';"
    sql "insert into d_table select 2,2,2,'b';"
    sql "insert into d_table select 3,-3,null,'c';"
    sql "insert into d_table select 3,-3,null,'c';"
    sql "insert into d_table select 3,-3,null,'c';"
    sql "insert into d_table select -4,-4,-4,'d';"
    sql "insert into d_table select -4,-4,-4,'d';"
    sql "insert into d_table select -4,-4,-4,'d';"

    create_sync_mv(db, "d_table", "mv_in_${db}", """
    select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from d_table group by abs(k1)+k2+1
    """)

    sql "analyze table d_table with sync;"
    sql """alter table d_table modify column k1 set stats ('row_count'='12');"""
    // use another db, mv rewrite should be correct
    sql """drop database IF EXISTS test_query_in_different_db"""

    sql """
    create database test_query_in_different_db; 
    """
    sql """
    use test_query_in_different_db; 
    """

    // query with index should success
    order_qt_select_with_index "select * from ${db}.d_table index mv_in_${db}"

    mv_rewrite_success("select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from ${db}.d_table group by abs(k1)+k2+1", "mv_in_${db}")
    order_qt_select_mv "select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from ${db}.d_table group by abs(k1)+k2+1;"
}
