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

suite("or_expansion") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_pipeline_engine = true"
    sql "drop table if exists oe1"
    sql "drop table if exists oe2"
    sql """
        CREATE TABLE IF NOT EXISTS oe1 (
            k0 bigint,
            k1 bigint
        )
        DUPLICATE KEY(k0)
        DISTRIBUTED BY HASH(k0) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1"
        )
    """

    sql """
        CREATE TABLE IF NOT EXISTS oe2 (
            k0 bigint,
            k1 bigint
        )
        DUPLICATE KEY(k0)
        DISTRIBUTED BY HASH(k0) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1"
        )
    """

    sql """
    alter table oe1 modify column k0 set stats ('row_count'='1000', 'ndv'='1000', 'min_value'='1', 'max_value'='1000', 'avg_size'='1000', 'max_size'='1000' )
    """
    sql """
    alter table oe2 modify column k0 set stats ('row_count'='1000', 'ndv'='1000', 'min_value'='1', 'max_value'='1000', 'avg_size'='1000', 'max_size'='1000' )
    """
    sql """
    alter table oe1 modify column k1 set stats ('row_count'='1000', 'ndv'='1000', 'min_value'='1000', 'max_value'='2000', 'avg_size'='1000', 'max_size'='1000' )
    """
    sql """
    alter table oe2 modify column k1 set stats ('row_count'='1000', 'ndv'='1000', 'min_value'='1000', 'max_value'='2000', 'avg_size'='1000', 'max_size'='1000' )
    """

    explain {
        sql("""
            select oe1.k0, oe2.k0
            from oe1 inner join oe2
            on oe1.k0 = oe2.k0 or oe1.k1 + 1 = oe2.k1 * 2
        """)
        contains "VHASH JOIN"
    }

    explain {
        sql("""
            select oe1.k0
            from oe1 left anti join oe2
            on oe1.k0 = oe2.k0 or oe1.k1 + 1 = oe2.k1 * 2
        """)
        contains "VHASH JOIN"
    }

    explain {
        sql("""
            select oe1.k0, oe2.k0
            from oe1 left outer join oe2
            on oe1.k0 = oe2.k0 or oe1.k1 + 1 = oe2.k1 * 2
        """)
        contains "VHASH JOIN"
    }

    explain {
        sql("""
            select oe1.k0, oe2.k0
            from oe1 full outer join oe2
            on oe1.k0 = oe2.k0 or oe1.k1 + 1 = oe2.k1 * 2
        """)
        contains "VHASH JOIN"
    }

    for (int i = 0; i < 10; i++) {
        sql "insert into oe1 values(${i}, ${i})"
        sql "insert into oe2 values(${i+20}, ${i+20})"
    }
    sql "insert into oe1 values(null, 1)"
    sql "insert into oe1 values(1, null)"
    sql "insert into oe1 values(null, null)"
    sql "insert into oe2 values(null, 1)"
    sql "insert into oe2 values(1, null)"
    sql "insert into oe2 values(null, null)"

    qt_order_ij """
        select oe1.k0, oe2.k0
        from oe1 inner join oe2
        on oe1.k0 = oe2.k0 or oe1.k1 + 1 = oe2.k1 * 2
        order by oe2.k0, oe1.k0
    """

    qt_order_laj """
            select oe1.k0
            from oe1 left anti join oe2
            on oe1.k0 = oe2.k0 or oe1.k1 + 1 = oe2.k1 * 2
            order by oe1.k0
        """

    qt_order_loj """
            select oe1.k0, oe2.k0
            from oe1 left outer join oe2
            on oe1.k0 = oe2.k0 or oe1.k1 + 1 = oe2.k1 * 2
            order by oe2.k0, oe1.k0
        """

    qt_order_foj """
            select oe1.k0, oe2.k0
            from oe1 full outer join oe2
            on oe1.k0 = oe2.k0 or oe1.k1 + 1 = oe2.k1 * 2
            order by oe2.k0, oe1.k0
        """
}
