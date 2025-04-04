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


/*
 * After running all regression-tests, check all db and tables
 * have same data.
 */
Suite.metaClass.check_multi_cluster_result = { clusterNames ->
    def isOrderBy = {String type ->
        // typeInfo likes
/*
+-----------+--------+------+-------+---------+--------------+
| Field     | Type   | Null | Key   | Default | Extra        |
+-----------+--------+------+-------+---------+--------------+
| datekey   | int    | Yes  | true  | NULL    |              |
*/
        type = type.toLowerCase()
        def noOrderByColumns = ["map", "struct", "hll", "json", "array", "bitmap", "variant", "agg_state", "quantile_state"]
        for (int i = 0; i < noOrderByColumns.size(); i++) {
            if (type.contains(noOrderByColumns[i])) {
                return false
            }
        }
        return true
    }

    def getSelectSql = { tableName ->
	def selectPart = "select "
        def orderbyPart = " order by "

        def syntaxMap = [
            "bitmap" : "bitmap_to_string(%s)",
            "map" : "noOrderBy"
        ]
        def columnTypes = sql_return_maparray """ show columns from ${tableName} """
        def hasComplexType = false        
        for (int i = 0; i < columnTypes.size(); i++) {
            if (isOrderBy(columnTypes[i]["Type"]) == false) {
                hasComplexType = true
                break
            }
        }

        if (hasComplexType == false) {
            selectPart = "select * "
            for (int i = 0; i < columnTypes.size(); i++) {
                if (i == 0) {
                   orderbyPart += " 1"
                } else {
                   orderbyPart += ",${i+1}"
                }
            }
            return selectPart + " from ${tableName} " + orderbyPart + " limit 100"
        }

        def firstSelect = true
        def firstOrderby = true
        def selectIndex = 0
        def skipKeyWords = ["key", "as", "by", "desc", "@timestamp"]
        for (int i = 0; i < columnTypes.size(); i++) {
           def name = columnTypes[i]["Field"]
           if (skipKeyWords.contains(name)) {
               continue
           }            
           selectIndex++
           def orderby = isOrderBy(columnTypes[i]["Type"])
           if (firstSelect) {
               selectPart += String.format("%s", name)
               firstSelect = false
           } else {
               selectPart += String.format(",%s", name)
           }

           if (orderby) {
               if (firstOrderby) {
                  orderbyPart += "${selectIndex}"
                  firstOrderby = false
               } else {
                  orderbyPart += ",${selectIndex}"
               }
           }
        }

        return selectPart + " from ${tableName} " + orderbyPart + " limit 100"
    }

    // main logic
    def clusters = clusterNames.tokenize(",")
    if (clusters.size() <= 1) {
        return
    }

    def ignore_dbs = ["mysql", "__internal_schema", "information_schema", "regression_test_datatype_p0_nested_types_base_cases", "regression_test_inverted_index_p0_array_contains", "regression_test_load_insert","regression_test_mtmv_p0", "regression_test_nereids_p0_sql_functions_array_functions","regression_test_nereids_p0_sql_functions_bitmap_functions", "regression_test_nereids_p0_sql_functions_datetime_functions", "regression_test_partition_p0_auto_partition","regression_test_query_p0_set_operations","regression_test_variant_p0","regression_test_variant_p0_rqg","test_env_db_dropped_mtmv_db2"]
    def dbs = sql """ show databases """
    dbs.forEach { db ->
        db = db[0]
        if (ignore_dbs.contains(db)) {
            return
        }
        sql """ use ${db} """
        def tables = sql """ show tables """
        tables.forEach { t -> 
            t = t[0]
            logger.info("process db: ${db}, table: ${t}")
            def q = getSelectSql(t)
            def result = []
            clusters.forEach { cluster ->
                sql """ use @${cluster} """
                result.add(sql """ ${q} """ )
            }

            if (result.toSet().size() != 1) {
                throw new IllegalArgumentException(""" different result, db: ${db}, table: ${t}. \n${result}  """)
            } 
        }
    }
}
