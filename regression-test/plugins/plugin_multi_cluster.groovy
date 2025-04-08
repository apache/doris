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

import org.apache.doris.regression.suite.Suite

/*
 * After running all regression-tests, check all db and tables
 * have same data in multi cluster deploy on cloud.
 */
Suite.metaClass.check_multi_cluster_result = { clusterNames ->
    def ignoreDBs = ["mysql", "__internal_schema", "information_schema"]
    // TODO: add skip reason.
    def specialTables = [
        "regression_test_function_p0" : ["mock_view": CompareSQLPattern.Skip,],
        "regression_test_nereids_syntax_p0" : ["test_show_keys_v" : CompareSQLPattern.Skip,],
        "regression_test_datatype_p0_nested_types_base_cases" : ["test_map_one_level": CompareSQLPattern.Skip,],
        "regression_test_inverted_index_p0_array_contains": ["httplogs_dup_arr": CompareSQLPattern.OrderByGroovy,],
        "nested_array_test_2_vectorized": ["nested_array_test_2_vectorized": CompareSQLPattern.OrderByGroovy],
        "regression_test_nereids_p0_sql_functions_bitmap_functions":["v1":CompareSQLPattern.OrderByGroovy],
        "regression_test_partition_p0_auto_partition": ["table_date_list" : CompareSQLPattern.OrderByGroovy],
        "regression_test_query_p0_aggregate": ["test_array_agg_complex": CompareSQLPattern.Skip],
        "regression_test_query_p0_set_operations": ["nested_array_test_2_vectorized" : CompareSQLPattern.Skip],
        "regression_test_variant_p0": ["multi_variants" : CompareSQLPattern.Skip],
        "regression_test_variant_p0_rqg": ["table_100_undef_partitions2_keys3_properties4_distributed_by53", CompareSQLPattern.Skip],
        "regression_test_variant_p0_with_index": ["test_variant_index_parser_empty" : CompareSQLPattern.Skip],
    ]
    enum CompareSQLPattern { Skip, OrderByGroovy, OrderBySQL }
    
    /*
     * return true if column type is a complex type.
     * type maybe like Map<int, String>, etc.
     */
    def isComplexColumn = {String type ->
        type = type.toLowerCase()
        def ComplexColumns = ["map", "struct", "hll", "json", "array", "bitmap", "variant", "agg_state", "quantile_state"]
        for (int i = 0; i < noOrderByColumns.size(); i++) {
            if (type.contains(noOrderByColumns[i])) {
                return false
            }
        }
        return true
    }
    
    /*
     * There are 2 compare methods:
     *  1. OrderByGroovy: retrieve all result from table and sort in groovy. like: sql 'select * from table', true
     *  2. OrderBySQL: retrieve sorted limited result from db, like: sql 'select * from table order by 1,2,3,4,5 limit 100'
     */
    def getOrderByType = { db, table ->
        def p = specialTables[db]?[t]
        if (p == null) {
            return CompareSQLPattern.OrderBySQL
        } else {
            return p
        }
    }

    def getSQL = { db, tableName ->
        def sqlStmt = "select * from ${tableName} order by "
        def columnTypes = sql_return_maparray """ show columns from ${tableName} """
        def firstOrderby = true
        for (int i = 0; i < columnTypes.size(); i++) {
            def name = columnTypes[i]["Field"]
            def type = columnTypes[i]["Type"]
            if (isComplexColumn(type)) {
                continue // can not add order by.
            } else {
                if (firstOrderby) {
                    firstOrderby = false
                    sqlStmt += "${i+1}"
                } else {
                    sqlStmt += ",${i+1}"
                }
            }
        }

        if (firstOrderby) {
            throw new IllegalArgumentException("""db: ${db}, table: ${tableName} is not suitable for orderBySQL """)
        }
        return sqlStmt + " limit 100"
    }

    def assertSameResult = { sqlStmt, order ->
        def result = []

        clusters.forEach { cluster ->
            sql """ use @${cluster} """
            result.add(sql """ ${q} """, order )
        }

        if (result.toSet().size() != 1) {
            throw new IllegalArgumentException(""" different result, db: ${db}, table: ${t}. \n${result}  """)
        }
    }

    // main logic
    def clusters = clusterNames.tokenize(",")
    if (clusters.size() <= 1) {
        return
    }

    int nErrors = 0
    def dbs = sql """ show databases """
    dbs.forEach { db ->
        db = db[0]
        if (ignoreDBs.contains(db)) {
            return
        }
        sql """ use ${db} """
        def tables = sql """ show tables """
        tables.forEach { t -> 
            try {
                t = t[0]
                logger.info("process db: ${db}, table: ${t}")
                
                def p = getOrderByType(db, t)
                if (p == CompareSQLPattern.OrderBySQL) {
                    assertSameResult(getSQL(db, t), false)
                } else if (p == CompareSQLPattern.Skip) {
                    return
                } else if (p == CompareSQLPattern.OrderByGroovy) {
                    assertSameResult("select * from ${t}", true)
                }
            } catch (Exception e) {
                nErrors++
                logger.info(""" asssertSameResult failed: """, e)
            }
        }
    }

    if (nErrors > 0) {
        throw new Exception(""" There are ${nErrors} met in assertSameResult """)
    }
}
