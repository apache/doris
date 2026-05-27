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

suite("topn_lazy_nested_column_pruning") {
    sql """ set topn_lazy_materialization_threshold=1024; """
    sql """ DROP TABLE IF EXISTS ncp_tbl """
    sql """
        CREATE TABLE ncp_tbl (
            id          INT,
            str_col     STRING NULL,
            struct_col  STRUCT<city: STRING, zip: INT> NULL,
            arr_col     ARRAY<INT> NULL,
            map_col     MAP<STRING, INT> NULL,
            int_col     INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    sql """
        INSERT INTO ncp_tbl VALUES
            (1, 'hello', named_struct('city', null, 'zip', 10001), [1, 2, 3], {'a': 1, 'b': 2 }, 1)
    """

        sql """
    drop table if exists vt;
    CREATE TABLE IF NOT EXISTS vt (
        id BIGINT NOT NULL,
        s varchar(100) null,
        payload VARIANT<
            'name' : STRING,
            'age' : INT
        > NULL
    )
    ENGINE = OLAP
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1",
        "storage_format" = "V3"
    );


    INSERT INTO vt VALUES
        (1, 'aaa', '{"name": "张三", "age": 25}'),
        (2, 'bbb', '{"name": "李四", "age": 30}'),
        (3, 'ccc', '{"name": "王五", "age": 28, "city": "北京"}');
    """

    // =============================================
    // Test 1: STRUCT type - lazy mat + nested column pruning
    // =============================================
    explain {
        sql """
            select id, substring(struct_element(struct_col, 'city'), 1) as city
            from ncp_tbl
            order by id
            limit 3
        """
        contains("VMaterializeNode")
        // struct_col lazy, scan only outputs id + rowId
        contains("final projections: id[#0], __DORIS_GLOBAL_ROWID_COL__ncp_tbl[#6]")
        // nested column pruning: struct_col pruned to city only
        contains("nested columns:")
        contains("pruned type: struct<city:text>")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__ncp_tbl]")
    }

    // =============================================
    // Test 2: STRUCT with select * - struct_col explicit in output
    // =============================================
    explain {
        sql """
            select *, substring(struct_element(struct_col, 'city'), 1) as city
            from ncp_tbl
            order by id
            limit 3
        """
        contains("VMaterializeNode")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__ncp_tbl]")
    }

    // =============================================
    // Test 3: VARIANT type - lazy mat + sub path pruning
    // =============================================
    explain {
        sql """
            select id, s, substring(element_at(payload, 'name'), 1) as name
            from vt
            order by id
            limit 3
        """
        contains("VMaterializeNode")
        // payload lazy, scan only outputs id + rowId
        contains("final projections: id[#0], __DORIS_GLOBAL_ROWID_COL__vt[#4]")
        // sub path pruning for variant
        contains("nested columns:")
        contains("sub path: [name]")
        contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__vt]")
    }

    // =============================================
    // Test 4: STRUCT - verify actual query results
    // =============================================
    qt_struct_result """
        select id, substring(struct_element(struct_col, 'city'), 1) as city
        from ncp_tbl
        order by id
        limit 3
    """

    // =============================================
    // Test 5: VARIANT - verify actual query results
    // =============================================
    qt_variant_result """
        select id, s, substring(element_at(payload, 'name'), 1) as name
        from vt
        order by id
        limit 3
    """
}
