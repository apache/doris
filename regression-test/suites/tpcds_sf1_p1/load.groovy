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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/tpcds
// and modified by Doris.
suite("load") {
    def tables=["store", "store_returns", "customer", "date_dim", "web_sales",
                "catalog_sales", "store_sales", "item", "web_returns", "catalog_returns",
                "catalog_page", "web_site", "customer_address", "customer_demographics",
                "ship_mode", "promotion", "inventory", "time_dim", "income_band",
                "call_center", "reason", "household_demographics", "warehouse", "web_page"]
    def columnsMap = [
        "item": """tmp_item_sk, tmp_item_id, tmp_rec_start_date, tmp_rec_end_date, tmp_item_desc,
                tmp_current_price, tmp_wholesale_cost, tmp_brand_id, tmp_brand, tmp_class_id, tmp_class,
                tmp_category_id, tmp_category, tmp_manufact_id, tmp_manufact, tmp_size, tmp_formulation,
                tmp_color, tmp_units, tmp_container, tmp_manager_id, tmp_product_name,
                i_item_sk=tmp_item_sk, i_item_id=tmp_item_id, i_rec_start_date=tmp_rec_start_date,
                i_rec_end_date=tmp_rec_end_date, i_item_desc=tmp_item_desc, i_current_price=tmp_current_price,
                i_wholesale_cost=tmp_wholesale_cost, i_brand_id=tmp_brand_id, i_brand=tmp_brand,
                i_class_id=tmp_class_id, i_class=tmp_class, i_category_id=tmp_category_id,
                i_category=nullif(tmp_category, ''), i_manufact_id=tmp_manufact_id, i_manufact=tmp_manufact,
                i_size=tmp_size, i_formulation=tmp_formulation, i_color=tmp_color, i_units=tmp_units,
                i_container=tmp_container, i_manager_id=tmp_manager_id, i_product_name=tmp_product_name""",

        "customer_address": """tmp_address_sk, tmp_address_id, tmp_street_number, tmp_street_name, tmp_street_type, tmp_suite_number,
                            tmp_city, tmp_county, tmp_state, tmp_zip, tmp_country, tmp_gmt_offset, tmp_location_type,
                            ca_address_sk=tmp_address_sk, ca_address_id=tmp_address_id, ca_street_number=tmp_street_number,
                            ca_street_name=tmp_street_name, ca_street_type=tmp_street_type, ca_suite_number=tmp_suite_number, ca_city=tmp_city,
                            ca_county=nullif(tmp_county, ''), ca_state=tmp_state, ca_zip=tmp_zip, ca_country=tmp_country,
                            ca_gmt_offset=tmp_gmt_offset, ca_location_type=tmp_location_type""",
    ]

    def specialTables = ["item", "customer_address"]

    for (String table in tables) {
        sql """ DROP TABLE IF EXISTS $table """
    }

    for (String table in tables) {
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    sql "set exec_mem_limit=8G;"

    for (String tableName in tables) {
        streamLoad {
            // you can skip db declaration, because a default db has already been
            // specified in ${DORIS_HOME}/conf/regression-conf.groovy
            // db 'regression_test'
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'

            if (specialTables.contains(tableName)) {
            set "columns", columnsMap[tableName]
            }


            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url()}/regression/tpcds/sf1/${tableName}.dat.gz"""

            time 10000 // limit inflight 10s

            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    Thread.sleep(70000) // wait for row count report of the tables just loaded
    for (String tableName in tables) {
        sql """ ANALYZE TABLE $tableName WITH SYNC """
    }
    sql """ sync """
}
