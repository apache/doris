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

suite("test_variant_external_meta_integration", "nonConcurrent") {
    def set_be_config = { key, value ->
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), 
                                                backendId_to_backendHttpPort.get(backend_id), 
                                                key, value)
        logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
    }

    sql "set enable_match_without_inverted_index = false"
    sql "set enable_common_expr_pushdown_for_inverted_index = true"
    sql "set enable_common_expr_pushdown = true"

    // Test 1: External meta with inverted index on extracted columns
    sql "DROP TABLE IF EXISTS test_inverted_index_extracted"
    sql """
        CREATE TABLE test_inverted_index_extracted (
            k bigint,
            v variant<'name':string, 'age':int, PROPERTIES ("variant_doc_materialization_min_rows" = "0")>,
            INDEX idx_v (v) USING INVERTED PROPERTIES("parser" = "english")
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    sql """insert into test_inverted_index_extracted values (1, '{"name": "Alice Smith", "age": 25}')"""
    sql """insert into test_inverted_index_extracted values (2, '{"name": "Bob Johnson", "age": 30}')"""
    sql """insert into test_inverted_index_extracted values (3, '{"name": "Charlie Smith", "age": 35}')"""
    
    // Test inverted index on name field (should work with external meta)
    qt_inverted_1 "select k, v['name'] from test_inverted_index_extracted where v['name'] match 'Smith' order by k"
    qt_inverted_2 "select k, v['name'] from test_inverted_index_extracted where v['name'] match 'Alice' order by k"
    qt_inverted_3 "select k, v['age'] from test_inverted_index_extracted where cast(v['age'] as int) > 25 order by k"
    
    // After compaction, inverted index should still work
    trigger_and_wait_compaction("test_inverted_index_extracted", "full", 1800)
    
    qt_inverted_after_compact_1 "select k, v['name'] from test_inverted_index_extracted where v['name'] match 'Smith' order by k"
    qt_inverted_after_compact_2 "select count(*) from test_inverted_index_extracted where v['name'] match 'Johnson'"

    // Test 2: External meta with parent column inverted index
    sql "DROP TABLE IF EXISTS test_parent_index"
    sql """
        CREATE TABLE test_parent_index (
            k bigint,
            v variant<properties("variant_max_subcolumns_count" = "5", "variant_doc_materialization_min_rows" = "0")>,
            INDEX idx_v (v) USING INVERTED PROPERTIES("parser" = "english")
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    sql """insert into test_parent_index values (1, '{"title": "Hello World", "content": "This is a test"}')"""
    sql """insert into test_parent_index values (2, '{"title": "Test Document", "content": "Another test here"}')"""
    sql """insert into test_parent_index values (3, '{"title": "World News", "content": "Breaking news today"}')"""
    
    // Query with inherited inverted index
    qt_parent_idx_1 "select k, v['title'] from test_parent_index where v['title'] match 'World' order by k"
    qt_parent_idx_2 "select k, v['content'] from test_parent_index where v['content'] match 'test' order by k"
    qt_parent_idx_3 "select count(*) from test_parent_index where v['title'] match 'Document'"

    // Test 3: Multiple variant columns with external meta
    sql "DROP TABLE IF EXISTS test_multiple_variants"
    sql """
        CREATE TABLE test_multiple_variants (
            k bigint,
            v1 variant,
            v2 variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    sql """insert into test_multiple_variants values (1, '{"a": 1, "b": 2}', '{"x": 10, "y": 20}')"""
    sql """insert into test_multiple_variants values (2, '{"a": 100, "c": 300}', '{"x": 1000, "z": 3000}')"""
    sql """insert into test_multiple_variants values (3, '{"d": 4}', '{"w": 40}')"""
    
    // Both variant columns should use external meta independently
    qt_multi_var_1 "select k, v1['a'], v2['x'] from test_multiple_variants order by k"
    qt_multi_var_2 "select k, v1['b'] from test_multiple_variants where cast(v1['b'] as int) is not null order by k"
    qt_multi_var_3 "select k, v2['y'] from test_multiple_variants where cast(v2['y'] as int) is not null order by k"
    qt_multi_var_4 "select k, v1['d'], v2['w'] from test_multiple_variants where cast(v1['d'] as int) is not null order by k"
    
    trigger_and_wait_compaction("test_multiple_variants", "full", 1800)
    
    qt_multi_var_after_compact_1 "select k, v1['a'], v2['x'] from test_multiple_variants order by k"
    qt_multi_var_after_compact_2 "select count(*) from test_multiple_variants"

    // Test 4: External meta with aggregation functions
    sql "DROP TABLE IF EXISTS test_aggregation"
    sql """
        CREATE TABLE test_aggregation (
            k bigint,
            category varchar(30),
            v variant
        )
        DUPLICATE KEY(`k`, `category`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "false", "storage_format" = "V3");
    """
    
    sql """insert into test_aggregation values (1, 'A', '{"value": 100, "count": 1}')"""
    sql """insert into test_aggregation values (2, 'A', '{"value": 200, "count": 2}')"""
    sql """insert into test_aggregation values (3, 'B', '{"value": 150, "count": 1}')"""
    sql """insert into test_aggregation values (4, 'B', '{"value": 250, "count": 3}')"""
    
    qt_agg_1 "select category, sum(cast(v['value'] as int)) from test_aggregation group by category order by category"
    qt_agg_2 "select category, avg(cast(v['count'] as double)) from test_aggregation group by category order by category"
    qt_agg_3 "select count(distinct category) from test_aggregation where cast(v['value'] as int) > 100"

    // Test 5: External meta with JOIN operations
    sql "DROP TABLE IF EXISTS test_join_left"
    sql "DROP TABLE IF EXISTS test_join_right"
    
    sql """
        CREATE TABLE test_join_left (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "false", "storage_format" = "V3");
    """
    
    sql """
        CREATE TABLE test_join_right (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "false", "storage_format" = "V3");
    """
    
    sql """insert into test_join_left values (1, '{"name": "Alice", "dept_id": 10}')"""
    sql """insert into test_join_left values (2, '{"name": "Bob", "dept_id": 20}')"""
    sql """insert into test_join_left values (3, '{"name": "Charlie", "dept_id": 10}')"""
    
    sql """insert into test_join_right values (10, '{"dept_name": "Engineering", "location": "Building A"}')"""
    sql """insert into test_join_right values (20, '{"dept_name": "Sales", "location": "Building B"}')"""
    
    qt_join_1 """
        select l.k, l.v['name'], r.v['dept_name']
        from test_join_left l 
        join test_join_right r on cast(l.v['dept_id'] as int) = r.k
        order by l.k
    """
    
    qt_join_2 """
        select count(*) 
        from test_join_left l 
        join test_join_right r on cast(l.v['dept_id'] as int) = r.k
        where r.v['location'] = 'Building A'
    """

    // Test 6: External meta with UNION operations
    sql "DROP TABLE IF EXISTS test_union_1"
    sql "DROP TABLE IF EXISTS test_union_2"
    
    sql """
        CREATE TABLE test_union_1 (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "false", "storage_format" = "V3");
    """
    
    sql """
        CREATE TABLE test_union_2 (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "false", "storage_format" = "V3");
    """
    
    sql """insert into test_union_1 values (1, '{"source": "table1", "value": 100}')"""
    sql """insert into test_union_1 values (2, '{"source": "table1", "value": 200}')"""
    
    sql """insert into test_union_2 values (3, '{"source": "table2", "value": 150}')"""
    sql """insert into test_union_2 values (4, '{"source": "table2", "value": 250}')"""
    
    qt_union_1 """
        select k, v['source'], v['value'] from test_union_1
        union all
        select k, v['source'], v['value'] from test_union_2
        order by k
    """
    
    qt_union_2 """
        select cast(v['source'] as string) as src, count(*) 
        from (
            select v from test_union_1
            union all
            select v from test_union_2
        ) t
        group by src
        order by src
    """

    // Test 7: External meta with subquery and WHERE clause
    sql "DROP TABLE IF EXISTS test_subquery"
    sql """
        CREATE TABLE test_subquery (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "false", "storage_format" = "V3");
    """
    
    for (int i = 1; i <= 10; i++) {
        sql """insert into test_subquery values (${i}, '{"score": ${i * 10}, "grade": "${i > 5 ? 'A' : 'B'}"}')"""
    }
    
    qt_subquery_1 """
        select k, v['score'], v['grade']
        from test_subquery
        where cast(v['score'] as int) > (select avg(cast(v['score'] as int)) from test_subquery)
        order by k
    """
    
    qt_subquery_2 """
        select cast(v['grade'] as string) as grade, count(*) as cnt
        from test_subquery
        where k in (select k from test_subquery where cast(v['score'] as int) >= 50)
        group by grade
        order by grade
    """

    // Test 8: External meta with HAVING clause
    sql "DROP TABLE IF EXISTS test_having"
    sql """
        CREATE TABLE test_having (
            k bigint,
            category varchar(30),
            v variant
        )
        DUPLICATE KEY(`k`, `category`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "false", "storage_format" = "V3");
    """
    
    sql """insert into test_having values (1, 'A', '{"amount": 100}')"""
    sql """insert into test_having values (2, 'A', '{"amount": 200}')"""
    sql """insert into test_having values (3, 'A', '{"amount": 300}')"""
    sql """insert into test_having values (4, 'B', '{"amount": 50}')"""
    sql """insert into test_having values (5, 'B', '{"amount": 60}')"""
    
    qt_having_1 """
        select category, sum(cast(v['amount'] as int)) as total
        from test_having
        group by category
        having total > 150
        order by category
    """

    // Test 9: External meta with ORDER BY and LIMIT on variant fields
    sql "DROP TABLE IF EXISTS test_order_limit"
    sql """
        CREATE TABLE test_order_limit (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "false", "storage_format" = "V3");
    """
    
    for (int i = 1; i <= 20; i++) {
        sql """insert into test_order_limit values (${i}, '{"priority": ${20 - i}, "name": "item_${i}"}')"""
    }
    
    qt_order_limit_1 """
        select k, v['priority'], v['name']
        from test_order_limit
        order by cast(v['priority'] as int) desc
        limit 5
    """
    
    qt_order_limit_2 """
        select k, v['name']
        from test_order_limit
        where cast(v['priority'] as int) > 10
        order by cast(v['priority'] as int)
        limit 3
    """

    // Test 10: External meta with DISTINCT on variant fields
    sql "DROP TABLE IF EXISTS test_distinct"
    sql """
        CREATE TABLE test_distinct (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "false", "storage_format" = "V3");
    """
    
    sql """insert into test_distinct values (1, '{"category": "A", "value": 100}')"""
    sql """insert into test_distinct values (2, '{"category": "B", "value": 200}')"""
    sql """insert into test_distinct values (3, '{"category": "A", "value": 150}')"""
    sql """insert into test_distinct values (4, '{"category": "C", "value": 100}')"""
    
    qt_distinct_1 "select distinct cast(v['category'] as string) from test_distinct order by cast(v['category'] as string)"
    qt_distinct_2 "select distinct cast(v['value'] as int) from test_distinct order by cast(v['value'] as int)"
    qt_distinct_3 "select count(distinct cast(v['category'] as string)) from test_distinct"

    // Cleanup
    sql "DROP TABLE IF EXISTS test_inverted_index_extracted"
    sql "DROP TABLE IF EXISTS test_parent_index"
    sql "DROP TABLE IF EXISTS test_multiple_variants"
    sql "DROP TABLE IF EXISTS test_aggregation"
    sql "DROP TABLE IF EXISTS test_join_left"
    sql "DROP TABLE IF EXISTS test_join_right"
    sql "DROP TABLE IF EXISTS test_union_1"
    sql "DROP TABLE IF EXISTS test_union_2"
    sql "DROP TABLE IF EXISTS test_subquery"
    sql "DROP TABLE IF EXISTS test_having"
    sql "DROP TABLE IF EXISTS test_order_limit"
    sql "DROP TABLE IF EXISTS test_distinct"
    
}


