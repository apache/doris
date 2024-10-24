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

suite("eliminate_inner") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET disable_join_reorder=true"

    sql """
        DROP TABLE IF EXISTS pkt
    """

    sql """
        DROP TABLE IF EXISTS fkt
    """
    sql """
        DROP TABLE IF EXISTS fkt_not_null
    """

    sql """
    CREATE TABLE IF NOT EXISTS pkt(
      `pk` int(11) NULL,
      `p` text NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(pk) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS fkt(
      `fk` int(11) NULL,
      `f` text NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(fk) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS fkt_not_null(
      `fk` int(11) NOT NULL,
      `f` text NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(fk) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
    INSERT INTO pkt VALUES (1, 'John'), (2, 'Alice'), (3, 'Bob'), (null, 'Jack');
    """
    sql """
    INSERT INTO fkt VALUES (1, 'John'), (2, 'Alice'), (null, 'Bob');
    """
    sql """
    INSERT INTO fkt_not_null VALUES (1, 'John'), (2, 'Alice'), (3, 'Bob');
    """
    sql """
    alter table pkt add constraint pk primary key (pk) 
    """
    sql """
    alter table fkt add constraint fk foreign key (fk) references pkt(pk) 
    """
    sql """
    alter table fkt_not_null add constraint fk foreign key (fk) references pkt(pk) 
    """
    def check_shape_res = { sql, name -> 
        qt_name "select \"${name}\""
        qt_shape "explain shape plan ${sql}"
        order_qt_res "${sql}"
    }

    // not nullable
    check_shape_res("select fkt_not_null.* from pkt inner join fkt_not_null on pkt.pk = fkt_not_null.fk;", "simple_case")
    check_shape_res("select fkt_not_null.*, pkt.pk from pkt inner join fkt_not_null on pkt.pk = fkt_not_null.fk;", "with_pk_col")
    check_shape_res("select fkt_not_null.*, pkt.pk from pkt inner join (select fkt_not_null1.* from fkt_not_null as fkt_not_null1 join fkt_not_null as fkt_not_null2 on fkt_not_null1.fk = fkt_not_null2.fk) fkt_not_null on pkt.pk = fkt_not_null.fk;", "with_pk_col")
    check_shape_res("select fkt_not_null.*, pkt.pk from pkt inner join (select fkt_not_null1.* from fkt_not_null as fkt_not_null1 join fkt_not_null as fkt_not_null2 on fkt_not_null1.fk = fkt_not_null2.fk where fkt_not_null1.fk > 1) fkt_not_null on pkt.pk = fkt_not_null.fk;", "with_pk_col")
    check_shape_res("select fkt_not_null.*, pkt.pk from pkt inner join (select fkt_not_null1.fk from fkt_not_null as fkt_not_null1 group by fkt_not_null1.fk) fkt_not_null on pkt.pk = fkt_not_null.fk;", "with_pk_col")
    check_shape_res("select fkt_not_null.*, pkt.pk from pkt inner join (select * from fkt_not_null union select * from fkt_not_null) fkt_not_null on pkt.pk = fkt_not_null.fk;", "with_pk_col")
    check_shape_res("select fkt_not_null.*, pkt.pk from pkt inner join (select fk, ROW_NUMBER() OVER (PARTITION BY fk ORDER BY fk) AS RowNum from fkt_not_null) fkt_not_null on pkt.pk = fkt_not_null.fk;", "fk with window")
    check_shape_res("select fkt_not_null.*, pkt.pk from pkt inner join (select fk from fkt_not_null order by fk limit 1) fkt_not_null on pkt.pk = fkt_not_null.fk;", "fk with limit")
    check_shape_res("select fkt_not_null.*, pkt.pk from pkt inner join fkt_not_null on pkt.pk = fkt_not_null.fk where pkt.pk = 1 and fkt_not_null.fk = 1;", "pk with filter that same as fk")
    check_shape_res("select fkt_not_null.*, pkt.pk from pkt inner join fkt_not_null on pkt.pk = fkt_not_null.fk where pkt.pk = 1 and fkt_not_null.fk = 1 and fkt_not_null.f = 1;", "pk with filter that included same as fk")
    check_shape_res("select fkt_not_null.*, pkt.pk from pkt inner join fkt_not_null on pkt.pk = fkt_not_null.fk where pkt.p = 1 and fkt_not_null.fk = 1 and fkt_not_null.f = 1;;", "pk with filter that not same as fk")
 
    // nullable
    check_shape_res("select fkt.* from pkt inner join fkt on pkt.pk = fkt.fk;", "simple_case")
    check_shape_res("select fkt.*, pkt.pk from pkt inner join fkt on pkt.pk = fkt.fk;", "with_pk_col")
    check_shape_res("select fkt.*, pkt.pk from pkt inner join (select fkt1.* from fkt as fkt1 join fkt as fkt2 on fkt1.fk = fkt2.fk) fkt on pkt.pk = fkt.fk;", "with_pk_col")
    check_shape_res("select fkt.*, pkt.pk from pkt inner join (select fkt1.* from fkt as fkt1 join fkt as fkt2 on fkt1.fk = fkt2.fk where fkt1.fk > 1) fkt on pkt.pk = fkt.fk;", "with_pk_col")
    check_shape_res("select fkt.*, pkt.pk from pkt inner join (select fkt1.fk from fkt as fkt1 group by fkt1.fk) fkt on pkt.pk = fkt.fk;", "with_pk_col")
    check_shape_res("select fkt.*, pkt.pk from pkt inner join (select * from fkt union select * from fkt) fkt on pkt.pk = fkt.fk;", "with_pk_col")
    check_shape_res("select fkt.*, pkt.pk from pkt inner join (select fk, ROW_NUMBER() OVER (PARTITION BY fk ORDER BY fk) AS RowNum from fkt) fkt on pkt.pk = fkt.fk;", "fk with window")
    check_shape_res("select fkt.*, pkt.pk from pkt inner join (select fk from fkt order by fk limit 1 ) fkt on pkt.pk = fkt.fk;", "fk with limit")
    check_shape_res("select fkt.*, pkt.pk from pkt inner join fkt on pkt.pk = fkt.fk where pkt.pk = 1 and fkt.fk = 1;", "pk with filter that same as fk")
    check_shape_res("select fkt.*, pkt.pk from pkt inner join fkt on pkt.pk = fkt.fk where pkt.pk = 1 and fkt.fk = 1 and fkt.f = 1;", "pk with filter that included same as fk")
    check_shape_res("select fkt.*, pkt.pk from pkt inner join fkt on pkt.pk = fkt.fk where pkt.p = 1 and fkt.fk = 1 and fkt.f = 1;", "pk with filter that not same as fk")

  // Test multiple table joins, where fkt_not_null and pkt have a primary key-foreign key relationship, and fkt_not_null2 is an foreign table.
  check_shape_res("select fkt_not_null.* from pkt inner join fkt_not_null on pkt.pk = fkt_not_null.fk inner join fkt_not_null as fkt_not_null2 on fkt_not_null.fk = fkt_not_null2.fk  where pkt.p = 1;", "multi_table_join_with_pk_predicate")

  // Test multiple table joins, where fkt_not_null and pkt have a primary key-foreign key relationship, fkt_not_null2 is an foreign table, and predicates exist in both tables.
  check_shape_res("select fkt_not_null.* from pkt inner join fkt_not_null on pkt.pk = fkt_not_null.fk inner join fkt_not_null as fkt_not_null2 on fkt_not_null.fk = fkt_not_null2.fk where pkt.pk = 1;", "multi_table_join_with_pk_fk_predicate")
}
