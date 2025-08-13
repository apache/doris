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


suite("test_variant_float_double_index", "p0, nonConcurrent"){
    def tableName = "test_variant_float_double_index"

    sql """ set describe_extend_variant_column = true """
    sql """ set enable_match_without_inverted_index = false """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set inverted_index_skip_threshold = 0 """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """

    def queryAndCheck = { String sqlQuery, int expectedFilteredRows = -1, boolean checkFilterUsed = true ->
      def checkpoints_name = "segment_iterator.inverted_index.filtered_rows"
      try {
          GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
          GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name, [filtered_rows: expectedFilteredRows])
          sql "set experimental_enable_parallel_scan = false"
          sql "sync"
          sql "${sqlQuery}"
      } finally {
          GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
          GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
      }
    }

    sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `v` variant<properties("variant_max_subcolumns_count" = "10")>,
            INDEX idx_variant (v) USING INVERTED
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")
    """
    
    sql """ insert into ${tableName} values (1, '{"float_col" : 1.5, "double_col" : 1.5239849328948}'), (2, '{"float_col" : 2.23, "double_col" : 2.239849328948}'), (3, '{"float_col" : 3.02, "double_col" : 3.029849328948}') """


    queryAndCheck("select count() from ${tableName} where v['double_col'] = 1.5239849328948", 2)
    queryAndCheck("select count() from ${tableName} where v['double_col'] = 2.239849328948", 2)
    queryAndCheck("select count() from ${tableName} where v['double_col'] = 3.029849328948", 2)
    queryAndCheck("select count() from ${tableName} where v['double_col'] > 1.5239849328949", 1)
    queryAndCheck("select count() from ${tableName} where v['double_col'] < 1.5239849328949", 2)
    queryAndCheck("select count() from ${tableName} where v['double_col'] in(1.5239849328948, 2.239849328948)", 1)


    queryAndCheck("select count() from ${tableName} where v['float_col'] = cast(1.5 as float)", 2)
    queryAndCheck("select count() from ${tableName} where v['float_col'] = cast(2.23 as float)", 2)
    queryAndCheck("select count() from ${tableName} where v['float_col'] = cast(3.02 as float)", 2)
    queryAndCheck("select count() from ${tableName} where v['float_col'] > cast(1.5 as float)", 1)
    queryAndCheck("select count() from ${tableName} where v['float_col'] < cast(1.6 as float)", 2)
    queryAndCheck("select count() from ${tableName} where v['float_col'] in(cast(1.5 as float), cast(2.23 as float))", 1)

    for (int i =0; i < 10; i++) {
        sql """ insert into ${tableName} values (1, '{"float_col" : 1.5, "double_col" : 1.5239849328948}'), (2, '{"float_col" : 2.23, "double_col" : 2.239849328948}'), (3, '{"float_col" : 3.02, "double_col" : 3.029849328948}') """
    }

    trigger_and_wait_compaction(tableName, "cumulative")
    
    queryAndCheck("select count() from ${tableName} where v['double_col'] = 1.5239849328948", 22)
    queryAndCheck("select count() from ${tableName} where v['double_col'] = 2.239849328948", 22)
    queryAndCheck("select count() from ${tableName} where v['double_col'] = 3.029849328948", 22)
    queryAndCheck("select count() from ${tableName} where v['double_col'] > 1.5239849328949", 11)
    queryAndCheck("select count() from ${tableName} where v['double_col'] < 1.5239849328949", 22)
    queryAndCheck("select count() from ${tableName} where v['double_col'] in(1.5239849328948, 2.239849328948)", 11)


    queryAndCheck("select count() from ${tableName} where v['float_col'] = cast(1.5 as float)", 22)
    queryAndCheck("select count() from ${tableName} where v['float_col'] = cast(2.23 as float)", 22)
    queryAndCheck("select count() from ${tableName} where v['float_col'] = cast(3.02 as float)", 22)
    queryAndCheck("select count() from ${tableName} where v['float_col'] > cast(1.5 as float)", 11)
    queryAndCheck("select count() from ${tableName} where v['float_col'] < cast(1.6 as float)", 22)
    queryAndCheck("select count() from ${tableName} where v['float_col'] in(cast(1.5 as float), cast(2.23 as float))", 11)


    
     sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `v` variant<
                MATCH_NAME 'float_col': float,
                MATCH_NAME 'double_col': double,
                properties("variant_max_subcolumns_count" = "10")
            >,
            INDEX idx_variant (v) USING INVERTED PROPERTIES ( "field_pattern" = "float_col"),
            INDEX idx_variant_double (v) USING INVERTED PROPERTIES ( "field_pattern" = "double_col")
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")
    """
    
    sql """ insert into ${tableName} values (1, '{"float_col" : 1.5, "double_col" : 1.5239849328948}'), (2, '{"float_col" : 2.23, "double_col" : 2.239849328948}'), (3, '{"float_col" : 3.02, "double_col" : 3.029849328948}') """

    qt_select "select variant_type(v) from ${tableName}"

    queryAndCheck("select count() from ${tableName} where v['double_col'] = 1.5239849328948", 2)
    queryAndCheck("select count() from ${tableName} where v['double_col'] = 2.239849328948", 2)
    queryAndCheck("select count() from ${tableName} where v['double_col'] = 3.029849328948", 2)
    queryAndCheck("select count() from ${tableName} where v['double_col'] > 1.5239849328949", 1)
    queryAndCheck("select count() from ${tableName} where v['double_col'] < 1.5239849328949", 2)
    queryAndCheck("select count() from ${tableName} where v['double_col'] in(1.5239849328948, 2.239849328948)", 1)

    queryAndCheck("select count() from ${tableName} where cast(v['float_col'] as float) = cast(1.5 as float)", 2)
    queryAndCheck("select count() from ${tableName} where cast(v['float_col'] as float) = cast(2.23 as float)", 2)
    queryAndCheck("select count() from ${tableName} where cast(v['float_col'] as float) = cast(3.02 as float)", 2)
    queryAndCheck("select count() from ${tableName} where cast(v['float_col'] as float) > cast(1.5 as float)", 1)
    queryAndCheck("select count() from ${tableName} where cast(v['float_col'] as float) < cast(1.6 as float)", 2)
    queryAndCheck("select count() from ${tableName} where cast(v['float_col'] as float) in(cast(1.5 as float), cast(2.23 as float))", 1)

    for (int i =0; i < 10; i++) {
        sql """ insert into ${tableName} values (1, '{"float_col" : 1.5, "double_col" : 1.5239849328948}'), (2, '{"float_col" : 2.23, "double_col" : 2.239849328948}'), (3, '{"float_col" : 3.02, "double_col" : 3.029849328948}') """
    }

    trigger_and_wait_compaction(tableName, "cumulative")
    
    queryAndCheck("select count() from ${tableName} where v['double_col'] = 1.5239849328948", 22)
    queryAndCheck("select count() from ${tableName} where v['double_col'] = 2.239849328948", 22)
    queryAndCheck("select count() from ${tableName} where v['double_col'] = 3.029849328948", 22)
    queryAndCheck("select count() from ${tableName} where v['double_col'] > 1.5239849328949", 11)
    queryAndCheck("select count() from ${tableName} where v['double_col'] < 1.5239849328949", 22)
    queryAndCheck("select count() from ${tableName} where v['double_col'] in(1.5239849328948, 2.239849328948)", 11)


    queryAndCheck("select count() from ${tableName} where cast(v['float_col'] as float) = cast(1.5 as float)", 22)
    queryAndCheck("select count() from ${tableName} where cast(v['float_col'] as float) = cast(2.23 as float)", 22)
    queryAndCheck("select count() from ${tableName} where cast(v['float_col'] as float) = cast(3.02 as float)", 22)
    queryAndCheck("select count() from ${tableName} where cast(v['float_col'] as float) > cast(1.5 as float)", 11)
    queryAndCheck("select count() from ${tableName} where cast(v['float_col'] as float) < cast(1.6 as float)", 22)
    queryAndCheck("select count() from ${tableName} where cast(v['float_col'] as float) in(cast(1.5 as float), cast(2.23 as float))", 11)
    
}