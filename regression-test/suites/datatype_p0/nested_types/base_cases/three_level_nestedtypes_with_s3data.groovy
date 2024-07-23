import org.apache.commons.lang3.StringUtils

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

suite("three_level_nestedtypes_with_s3data") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def dataFilePath = "https://"+"${bucket}"+"."+"${s3_endpoint}"+"/regression/datalake"
//    def dataFilePath = "/mnt/disk1/wangqiannan/export/tl/three_level"
    def table_names = [
            "three_level_array_array_array",
            "three_level_array_array_map",
            "three_level_array_array_struct",
            "three_level_array_map_array",
            "three_level_array_map_map",
            "three_level_array_map_struct",
            "three_level_array_struct_array",
            "three_level_array_struct_map",
            "three_level_array_struct_struct",

            "three_level_map_array_array",
            "three_level_map_array_map",
            "three_level_map_array_struct",
            "three_level_map_map_array",
            "three_level_map_map_map",
            "three_level_map_map_struct",
            "three_level_map_struct_array",
            "three_level_map_struct_map",
            "three_level_map_struct_struct",

            "three_level_struct_array_array",
            "three_level_struct_array_map",
            "three_level_struct_array_struct",
            "three_level_struct_map_array",
            "three_level_struct_map_map",
            "three_level_struct_map_struct",
            "three_level_struct_struct_array",
            "three_level_struct_struct_map",
            "three_level_struct_struct_struct"
    ]

    def colNameArr = ["c_bool", "c_tinyint", "c_smallint", "c_int", "c_bigint", "c_largeint", "c_float",
                      "c_double", "c_decimal", "c_decimalv3", "c_date", "c_datetime", "c_datev2", "c_datetimev2",
                      "c_char", "c_varchar", "c_string"]

    def groupby_or_orderby_exception = {is_groupby, table_name, col_name ->
        test {
            if (is_groupby) {
                sql "select ${col_name} from ${table_name} group by ${col_name};"
            } else {
                sql "select ${col_name} from ${table_name} order by ${col_name};"
            }
            exception("errCode = 2, detailMessage = Doris hll, bitmap, array, map, struct, jsonb, variant column must use with specific function, and don't support filter, group by or order by")
        }
    }

    def select_nested_scala_element_at = { agg_expr, table_name ->
        order_qt_select_nested "select ${agg_expr} from ${table_name} where ${agg_expr} IS NOT NULL AND k1 IS NOT NULL order by k1 limit 10;"
    }

    def groupby_or_orderby_element_at = {is_groupby, table_name, agg_expr ->
        if (is_groupby) {
            order_qt_sql "select ${agg_expr} from ${table_name} where k1 IS NOT NULL group by ${agg_expr};"
        } else {
            order_qt_sql "select ${agg_expr} from ${table_name} where k1 IS NOT NULL order by ${agg_expr} limit 10;"
        }
    }

    def be_id = 10044
    def load_from_tvf = {table_name, uri_file, format ->
        if (format == "csv") {
            order_qt_sql_tvf """select c2, c9, c10, c12, c16 from local(
                "file_path" = "${uri_file}",
                "backend_id" = "${be_id}",
                "column_separator"="|",
                "format" = "${format}") order by c1 limit 10; """
            sql """
            insert into ${table_name} select * from local(
            "file_path" = "${uri_file}",
            "backend_id" = "${be_id}",
            "column_separator"="|",
            "format" = "${format}") order by c1; """
        } else {
            order_qt_sql_tvf """select c_bool, c_double, c_decimal, c_date, c_char from local(
                "file_path" = "${uri_file}",
                "backend_id" = "${be_id}",
                "column_separator"="|",
                "format" = "${format}") order by k1 limit 10;"""
            sql """
            insert into ${table_name} select * from local(
            "file_path" = "${uri_file}",
            "backend_id" = "${be_id}",
            "column_separator"="|",
            "format" = "${format}") order by k1; """
        }

        // where to filter different format data
        order_qt_select_doris """ select c_bool, c_double, c_decimal, c_date, c_char from ${table_name} where k1 IS NOT NULL order by k1 limit 10; """
    }
    def load_from_s3 = {table_name, uri_file, format ->
        if (format == "csv") {
            sql """
            insert into ${table_name} select * from s3(
            "uri" = "${uri_file}",
                    "s3.access_key"= "${ak}",
                    "s3.secret_key" = "${sk}",
                    "format" = "${format}",
                    "column_separator"="|",
                    "provider" = "${getS3Provider()}",
                    "read_json_by_line"="true") order by c1; """
        } else {
            sql """
            insert into ${table_name} select * from s3(
            "uri" = "${uri_file}",
                    "s3.access_key"= "${ak}",
                    "s3.secret_key" = "${sk}",
                    "format" = "${format}",
                    "provider" = "${getS3Provider()}",
                    "read_json_by_line"="true") order by k1; """
        }
        // where to filter different format data
        qt_select_doris """ select c_bool, c_double, c_decimal, c_date, c_char from ${table_name} where k1 IS NOT NULL order by k1 limit 10; """
    }

    // step1. create table
    // step2. load from s3
    //      step 2.1 format: parquet|orc|json|csv
    // step3. select *
    // step4. select element_at(column in first, -1(last), null, 0)
    // step5. select * from table where element_at(column) equals expr just ok
    // step6. select * from table where groupby|orderby column
    // step7. select * from table where groupby|orderby element_at(column)

    def format_order = [
            "parquet",
//            "orc",
//            "json",
            "csv"]
    // create tables
    // array (0,0,0) (0,0,1) (0,0,2) (0,1,0) (0,1,1) (0,1,2) (0,2,0) (0,2,1) (0,2,2)
    // map (1,0,0) (1,0,1) (1,0,2) (1,1,0) (1,1,1) (1,1,2) (1,2,0) (1,2,1) (1,2,2)
    // struct (2,0,0) (2,0,1) (2,0,2) (2,1,0) (2,1,1) (2,1,2) (2,2,0) (2,2,1) (2,2,2)
    def table_idx = 0
    for (int i = 0; i < 3; ++i) {
        for (int j = 0; j < 3; ++j) {
            for ( int k = 0; k < 3; ++k) {
                sql """ DROP TABLE IF EXISTS ${table_names[table_idx]} """
                String result = create_table_with_nested_type(3, [i, j, k], table_names[table_idx])
                sql result
                ++ table_idx
            }
        }
    }

    //========================= ARRAY =========================
    // insert into doris table
    ArrayList<String> array_files = [
            // array-array-array array-array-map array-array-struct
            "${dataFilePath}/three_level_array_array_array.parquet",
//                                    "${dataFilePath}/three_level_array_array.orc",
//                                    "${dataFilePath}/three_level_array_array.json",
            "${dataFilePath}/three_level_array_array_array.csv",
            "${dataFilePath}/three_level_array_array_map.parquet",
//                                    "${dataFilePath}/three_level_array_array_map.orc",
//                                    "${dataFilePath}/three_level_array_array_map.json",
            "${dataFilePath}/three_level_array_array_map.csv",
            "${dataFilePath}/three_level_array_array_struct.parquet",
//                                    "${dataFilePath}/three_level_array_array_struct.orc",
//                                    "${dataFilePath}/three_level_array_array_struct.json",
            "${dataFilePath}/three_level_array_array_struct.csv",

            // array-map-array array-map-map array-map-struct
            "${dataFilePath}/three_level_array_map_array.parquet",
//                                    "${dataFilePath}/three_level_array_map_array.orc",
//                                    "${dataFilePath}/three_level_array_map_array.json",
            "${dataFilePath}/three_level_array_map_array.csv",
            "${dataFilePath}/three_level_array_map_map.parquet",
//                                    "${dataFilePath}/three_level_array_map_map.orc",
//                                    "${dataFilePath}/three_level_array_map_map.json",
            "${dataFilePath}/three_level_array_map_map.csv",
            "${dataFilePath}/three_level_array_map_struct.parquet",
//                                    "${dataFilePath}/three_level_array_map_struct.orc",
//                                    "${dataFilePath}/three_level_array_map_struct.json",
            "${dataFilePath}/three_level_array_map_struct.csv",

            // array-struct-array array-struct-map array-struct-struct
            "${dataFilePath}/three_level_array_struct_array.parquet",
//                                    "${dataFilePath}/three_level_array_struct_array.orc",
//                                    "${dataFilePath}/three_level_array_struct_array.json",
            "${dataFilePath}/three_level_array_struct_array.csv",
            "${dataFilePath}/three_level_array_struct_map.parquet",
//                                    "${dataFilePath}/three_level_array_struct_map.orc",
//                                    "${dataFilePath}/three_level_array_struct_map.json",
            "${dataFilePath}/three_level_array_struct_map.csv",
            "${dataFilePath}/three_level_array_struct_struct.parquet",
//                                    "${dataFilePath}/three_level_array_struct_struct.orc",
//                                    "${dataFilePath}/three_level_array_struct_struct.json",
            "${dataFilePath}/three_level_array_struct_struct.csv",
    ]

    int ffi = 0
    for (int ti = 0; ti < 9; ++ti) {
        // array-array-array, array-array-map, array-array-struct,
        // array-map-array, array-map-map, array-map-struct,
        // array-struct-array, array-struct-map, array-struct-struct
        String table_name = table_names[ti]
        for (int fi = 0; fi < format_order.size(); ++fi) {
            String form = format_order[fi]
            sql "truncate table ${table_name};"
//            load_from_tvf(table_name, array_files[ffi], form)
            load_from_s3(table_name, array_files[ffi], form)
            ++ ffi
        }
        qt_select_count_array """ select count() from ${table_name}; """
    }

    for (int i = 0; i < 9; ++i) {
        String table_name = table_names[i]
        // select element_at(column)
        for (String col : colNameArr) {
            // first
            order_qt_select_arr "select ${col}[1] from ${table_name} where k1 IS NOT NULL order by k1 limit 10;"
            // last
            order_qt_select_arr "select ${col}[-1] from ${table_name} where k1 IS NOT NULL order by k1 limit 10;"
            // null
            order_qt_select_arr_null "select ${col}[0] from ${table_name} where k1 IS NOT NULL order by k1 limit 10;"
            // null
            order_qt_select_arr_null "select ${col}[1000] from ${table_name} where k1 IS NOT NULL order by k1 limit 10;"
        }
        // select * from table where element_at(column) with equal expr
        for (String col : colNameArr) {
            order_qt_select_arr "select ${col}[1], ${col}[-1] from ${table_name} where k1 IS NOT NULL order by k1 limit 10;"
        }
        // select * from table where groupby|orderby column will meet exception
        for (String col : colNameArr) {
            groupby_or_orderby_exception(true, table_name, col)
            groupby_or_orderby_exception(false, table_name, col)

            String agg_expr = "${col}[1]"
            groupby_or_orderby_exception(true, table_name, agg_expr)
            groupby_or_orderby_exception(false, table_name, agg_expr)
        }
    }

    // most-nested-column
    // array-array-array
    String agg_expr = "${colNameArr[0]}[1][1][1]"
    select_nested_scala_element_at(agg_expr, table_names[0])
    groupby_or_orderby_element_at(true, table_names[0], agg_expr)
    groupby_or_orderby_element_at(false, table_names[0], agg_expr)
    // array-array-map
    agg_expr = "${colNameArr[0]}[1][1][map_keys(${colNameArr[0]}[1][1])[1]]"
    select_nested_scala_element_at(agg_expr, table_names[1])
    groupby_or_orderby_element_at(true, table_names[1], agg_expr)
    groupby_or_orderby_element_at(false, table_names[1], agg_expr)
    // array-array-struct
    // select element_at(column)
    order_qt_select_arr "select struct_element(${colNameArr[0]}[1][1], 1), struct_element(${colNameArr[0]}[1][1], 'col17') from ${table_names[2]} where k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where element_at(column) with equal expr
    order_qt_select_arr "select struct_element(${colNameArr[0]}[1][1], 1), struct_element(${colNameArr[0]}[1][1], 'col17') from ${table_names[2]} where struct_element(${colNameArr[0]}[1][1], 1) IS NOT NULL AND k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where groupby|orderby column will meet exception
    groupby_or_orderby_exception(true, table_names[2], colNameArr[0])
    groupby_or_orderby_exception(false, table_names[2], colNameArr[0])
    // select * from table where groupby|orderby element_at(column)
    agg_expr = "struct_element(${colNameArr[0]}[1][1], 1)"
    groupby_or_orderby_element_at(true, table_names[2], agg_expr)
    groupby_or_orderby_element_at(false, table_names[2], agg_expr)

    // array-map-array
    agg_expr = "${colNameArr[0]}[1][map_keys(${colNameArr[0]}[1])[1]][1]"
    select_nested_scala_element_at(agg_expr, table_names[3])
    groupby_or_orderby_element_at(true, table_names[3], agg_expr)
    groupby_or_orderby_element_at(false, table_names[3], agg_expr)
    // array-map-map
    agg_expr = "${colNameArr[0]}[1][map_keys(${colNameArr[0]}[1])[1]][map_keys(${colNameArr[0]}[1][map_keys(${colNameArr[0]}[1])[1]])[1]]"
    select_nested_scala_element_at(agg_expr, table_names[4])
    groupby_or_orderby_element_at(true, table_names[4], agg_expr)
    groupby_or_orderby_element_at(false, table_names[4], agg_expr)
    // array-map-struct
    // select element_at(column)
    order_qt_select_arr "select struct_element(${colNameArr[0]}[1][map_keys(${colNameArr[0]}[1])[1]], 1), struct_element(${colNameArr[0]}[1][map_keys(${colNameArr[0]}[1])[1]], 'col17') from ${table_names[5]} where k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where element_at(column) with equal expr
    order_qt_select_arr "select struct_element(${colNameArr[0]}[1][map_keys(${colNameArr[0]}[1])[1]], 1), struct_element(${colNameArr[0]}[1][map_keys(${colNameArr[0]}[1])[1]], 'col17') from ${table_names[5]} where struct_element(${colNameArr[0]}[1][map_keys(${colNameArr[0]}[1])[1]], 1) IS NOT NULL AND k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where groupby|orderby column will meet exception
    groupby_or_orderby_exception(true, table_names[5], colNameArr[0])
    groupby_or_orderby_exception(false, table_names[5], colNameArr[0])
    // select * from table where groupby|orderby element_at(column)
    agg_expr = "struct_element(${colNameArr[0]}[1][map_keys(${colNameArr[0]}[1])[1]], 1)"
    groupby_or_orderby_element_at(true, table_names[5], agg_expr)
    groupby_or_orderby_element_at(false, table_names[5], agg_expr)

    // array-struct-array
    agg_expr = "struct_element(${colNameArr[0]}[1], 1)[1]"
    select_nested_scala_element_at(agg_expr, table_names[6])
    groupby_or_orderby_element_at(true, table_names[6], agg_expr)
    groupby_or_orderby_element_at(false, table_names[6], agg_expr)
    // array-struct-map
    agg_expr = "struct_element(${colNameArr[0]}[1], 1)[map_keys(struct_element(${colNameArr[0]}[1], 1))[1]]"
    select_nested_scala_element_at(agg_expr, table_names[7])
    groupby_or_orderby_element_at(true, table_names[7], agg_expr)
    groupby_or_orderby_element_at(false, table_names[7], agg_expr)
    // array-struct-struct
    // select element_at(column)
    order_qt_select_arr "select struct_element(struct_element(${colNameArr[0]}[1], 1), 1), struct_element(struct_element(${colNameArr[0]}[1], 1), 'col17') from ${table_names[8]} where k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where element_at(column) with equal expr
    order_qt_select_arr "select struct_element(struct_element(${colNameArr[0]}[1], 1), 1), struct_element(struct_element(${colNameArr[0]}[1], 1), 'col17') from ${table_names[8]} where struct_element(struct_element(${colNameArr[0]}[1], 1), 1) IS NOT NULL AND k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where groupby|orderby column will meet exception
    groupby_or_orderby_exception(true, table_names[8], colNameArr[0])
    groupby_or_orderby_exception(false, table_names[8], colNameArr[0])
    // select * from table where groupby|orderby element_at(column)
    agg_expr = "struct_element(struct_element(${colNameArr[0]}[1], 1), 1)"
    groupby_or_orderby_element_at(true, table_names[8], agg_expr)
    groupby_or_orderby_element_at(false, table_names[8], agg_expr)

    //========================== MAP ==========================
    // insert into doris table
    ArrayList<String> map_files = [
            // map-array-array map-array-map map-array-struct
            "${dataFilePath}/three_level_map_array_array.parquet",
//                                    "${dataFilePath}/three_level_map_array_array.orc",
//                                    "${dataFilePath}/three_level_map_array_array.json",
            "${dataFilePath}/three_level_map_array_array.csv",
            "${dataFilePath}/three_level_map_array_map.parquet",
//                                    "${dataFilePath}/three_level_map_array_map.orc",
//                                    "${dataFilePath}/three_level_map_array_map.json",
            "${dataFilePath}/three_level_map_array_map.csv",
            "${dataFilePath}/three_level_map_array_struct.parquet",
//                                    "${dataFilePath}/three_level_map_array_struct.orc",
//                                    "${dataFilePath}/three_level_map_array_struct.json",
            "${dataFilePath}/three_level_map_array_struct.csv",

            // map-map-array map-map-map map-map-struct
            "${dataFilePath}/three_level_map_map_array.parquet",
//                                    "${dataFilePath}/three_level_map_map_array.orc",
//                                    "${dataFilePath}/three_level_map_map_array.json",
            "${dataFilePath}/three_level_map_map_array.csv",
            "${dataFilePath}/three_level_map_map_map.parquet",
//                                    "${dataFilePath}/three_level_map_map_map.orc",
//                                    "${dataFilePath}/three_level_map_map_map.json",
            "${dataFilePath}/three_level_map_map_map.csv",
            "${dataFilePath}/three_level_map_map_struct.parquet",
//                                    "${dataFilePath}/three_level_map_map_struct.orc",
//                                    "${dataFilePath}/three_level_map_map_struct.json",
            "${dataFilePath}/three_level_map_map_struct.csv",

            // map-struct-array map-struct-map map-struct-struct
            "${dataFilePath}/three_level_map_struct_array.parquet",
//                                    "${dataFilePath}/three_level_map_struct_array.orc",
//                                    "${dataFilePath}/three_level_map_struct_array.json",
            "${dataFilePath}/three_level_map_struct_array.csv",
            "${dataFilePath}/three_level_map_struct_map.parquet",
//                                    "${dataFilePath}/three_level_map_struct_map.orc",
//                                    "${dataFilePath}/three_level_map_struct_map.json",
            "${dataFilePath}/three_level_map_struct_map.csv",
            "${dataFilePath}/three_level_map_struct_struct.parquet",
//                                    "${dataFilePath}/three_level_map_struct_struct.orc",
//                                    "${dataFilePath}/three_level_map_struct_struct.json",
            "${dataFilePath}/three_level_map_struct_struct.csv",

    ]

    ffi = 0
    for (int ti = 9; ti < 18; ++ti) {
        // map-array-array, map-array-map, map-array-struct,
        // map-map-array, map-map-map, map-map-struct,
        // map-struct-array, map-struct-map, map-struct-struct
        String table_name = table_names[ti]
        for (int fi = 0; fi < format_order.size(); ++fi) {
            String form = format_order[fi]
            sql "truncate table ${table_name};"
//            load_from_tvf(table_name, map_files[ffi], form)
            load_from_s3(table_name, map_files[ffi], form)
            ++ ffi
        }
        qt_select_count_map """ select count() from ${table_name}; """
    }
    for (int i = 9; i < 18; ++i ) {
        // map-array-array, map-array-map, map-array-struct,
        // map-map-array, map-map-map, map-map-struct,
        // map-struct-array, map-struct-map, map-struct-struct
        String table_name = table_names[i]
        // select element_at(column)
        for (String col : colNameArr) {
            // first
            order_qt_select_map "select ${col}[map_keys(${col})[1]] from ${table_name} where k1 IS NOT NULL order by k1 limit 10;"
            // last
            order_qt_select_map "select ${col}[map_keys(${col})[-1]] from ${table_name} where k1 IS NOT NULL order by k1 limit 10;"
            // null
            order_qt_select_map_null "select ${col}[map_keys(${col})[0]] from ${table_name} where k1 IS NOT NULL order by k1 limit 10;"
            // null
            order_qt_select_map_null "select ${col}[map_keys(${col})[1000]] from ${table_name} where k1 IS NOT NULL order by k1 limit 10;"
        }
        // select * from table where element_at(column) with equal expr
        for (String col : colNameArr) {
            order_qt_select_map "select ${col}[map_keys(${col})[1]], ${col}[map_keys(${col})[-1]] from ${table_name} where k1 IS NOT NULL order by k1 limit 10;"
        }
        // select * from table where groupby|orderby column will meet exception
        for (String col : colNameArr) {
            groupby_or_orderby_exception(true, table_names[i], col)
            groupby_or_orderby_exception(false, table_names[i], col)
        }
        // select * from table where groupby|orderby element_at(column)
        for (String col : colNameArr) {
            agg_expr = "${col}[map_keys(${col})[1]]"
            groupby_or_orderby_exception(true, table_names[i], agg_expr)
            groupby_or_orderby_exception(false, table_names[i], agg_expr)
        }
    }
    // most-nested-column
    // map-array-array
    agg_expr = "${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][1][1]"
    select_nested_scala_element_at(agg_expr, table_names[9])
    groupby_or_orderby_element_at(true, table_names[9], agg_expr)
    groupby_or_orderby_element_at(false, table_names[9], agg_expr)
    // map-array-map
    agg_expr = "${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][1][map_keys(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][1])[1]]"
    select_nested_scala_element_at(agg_expr, table_names[10])
    groupby_or_orderby_element_at(true, table_names[10], agg_expr)
    groupby_or_orderby_element_at(false, table_names[10], agg_expr)
    // map-array-struct
    // select element_at(column)
    order_qt_select_map "select struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][1], 1), struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][1], 'col17') from ${table_names[11]} where k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where element_at(column) with equal expr
    order_qt_select_map "select struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][1], 1), struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][1], 'col17') from ${table_names[11]} where struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][1], 1) IS NOT NULL AND k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where groupby|orderby column will meet exception
    groupby_or_orderby_exception(true, table_names[11], colNameArr[0])
    groupby_or_orderby_exception(false, table_names[11], colNameArr[0])
    // select * from table where groupby|orderby element_at(column)
    agg_expr = "struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][1], 1)"
    groupby_or_orderby_element_at(true, table_names[11], agg_expr)
    groupby_or_orderby_element_at(false, table_names[11], agg_expr)

    // map-map-array
    agg_expr = "${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][map_keys(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]])[1]][1]"
    select_nested_scala_element_at(agg_expr, table_names[12])
    groupby_or_orderby_element_at(true, table_names[12], agg_expr)
    groupby_or_orderby_element_at(false, table_names[12], agg_expr)
    // map-map-map
    agg_expr = "${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][map_keys(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]])[1]][map_keys(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][map_keys(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]])[1]])[1]]"
    select_nested_scala_element_at(agg_expr, table_names[13])
    groupby_or_orderby_element_at(true, table_names[13], agg_expr)
    groupby_or_orderby_element_at(false, table_names[13], agg_expr)
    // map-map-struct
    // select element_at(column)
    order_qt_select_map "select struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][map_keys(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]])[1]], 1), struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][map_keys(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]])[1]], 'col17') from ${table_names[14]} where k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where element_at(column) with equal expr
    order_qt_select_map "select struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][map_keys(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]])[1]], 1), struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][map_keys(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]])[1]], 'col17') from ${table_names[14]} where struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][map_keys(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]])[1]], 1) IS NOT NULL AND k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where groupby|orderby column will meet exception
    groupby_or_orderby_exception(true, table_names[14], colNameArr[0])
    groupby_or_orderby_exception(false, table_names[14], colNameArr[0])
    // select * from table where groupby|orderby element_at(column)
    agg_expr = "struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]][map_keys(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]])[1]], 1)"
    groupby_or_orderby_element_at(true, table_names[14], agg_expr)
    groupby_or_orderby_element_at(false, table_names[14], agg_expr)

    // map-struct-array
    agg_expr = "struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]], 1)[1]"
    select_nested_scala_element_at(agg_expr, table_names[15])
    groupby_or_orderby_element_at(true, table_names[15], agg_expr)
    groupby_or_orderby_element_at(false, table_names[15], agg_expr)
    // map-struct-map
    agg_expr = "struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]], 1)[map_keys(struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]], 1))[1]]"
    select_nested_scala_element_at(agg_expr, table_names[16])
    groupby_or_orderby_element_at(true, table_names[16], agg_expr)
    groupby_or_orderby_element_at(false, table_names[16], agg_expr)
    // map-struct-struct
    // select element_at(column)
    order_qt_select_map "select struct_element(struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]], 1), 1), struct_element(struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]], 1), 'col17') from ${table_names[17]} where k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where element_at(column) with equal expr
    order_qt_select_map "select struct_element(struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]], 1), 1), struct_element(struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]], 1), 'col17') from ${table_names[17]} where struct_element(struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]], 1), 1) IS NOT NULL AND k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where groupby|orderby column will meet exception
    groupby_or_orderby_exception(true, table_names[17], colNameArr[0])
    groupby_or_orderby_exception(false, table_names[17], colNameArr[0])
    // select * from table where groupby|orderby element_at(column)
    agg_expr = "struct_element(struct_element(${colNameArr[0]}[map_keys(${colNameArr[0]})[1]], 1), 1)"
    groupby_or_orderby_element_at(true, table_names[17], agg_expr)
    groupby_or_orderby_element_at(false, table_names[17], agg_expr)


    //========================= STRUCT ========================
    // insert into doris table
    ArrayList<String> struct_files = [
            // struct-array-array struct-array-map struct-array-struct
            "${dataFilePath}/three_level_struct_array_array.parquet",
//                                    "${dataFilePath}/three_level_struct_array_array.orc",
//                                    "${dataFilePath}/three_level_struct_array_array.json",
            "${dataFilePath}/three_level_struct_array_array.csv",
            "${dataFilePath}/three_level_struct_array_map.parquet",
//                                    "${dataFilePath}/three_level_struct_array_map.orc",
//                                    "${dataFilePath}/three_level_struct_array_map.json",
            "${dataFilePath}/three_level_struct_array_map.csv",
            "${dataFilePath}/three_level_struct_array_struct.parquet",
//                                    "${dataFilePath}/three_level_struct_array_struct.orc",
//                                    "${dataFilePath}/three_level_struct_array_struct.json",
            "${dataFilePath}/three_level_struct_array_struct.csv",

            // struct-map-array struct-map-map struct-map-struct
            "${dataFilePath}/three_level_struct_map_array.parquet",
//                                    "${dataFilePath}/three_level_struct_map_array.orc",
//                                    "${dataFilePath}/three_level_struct_map_array.json",
            "${dataFilePath}/three_level_struct_map_array.csv",
            "${dataFilePath}/three_level_struct_map_map.parquet",
//                                    "${dataFilePath}/three_level_struct_map_map.orc",
//                                    "${dataFilePath}/three_level_struct_map_map.json",
            "${dataFilePath}/three_level_struct_map_map.csv",
            "${dataFilePath}/three_level_struct_map_struct.parquet",
//                                    "${dataFilePath}/three_level_struct_map_struct.orc",
//                                    "${dataFilePath}/three_level_struct_map_struct.json",
            "${dataFilePath}/three_level_struct_map_struct.csv",

            // struct-struct-array struct-struct-map struct-struct-struct
            "${dataFilePath}/three_level_struct_struct_array.parquet",
//                                    "${dataFilePath}/three_level_struct_struct_array.orc",
//                                    "${dataFilePath}/three_level_struct_struct_array.json",
            "${dataFilePath}/three_level_struct_struct_array.csv",
            "${dataFilePath}/three_level_struct_struct_map.parquet",
//                                    "${dataFilePath}/three_level_struct_struct_map.orc",
//                                    "${dataFilePath}/three_level_struct_struct_map.json",
            "${dataFilePath}/three_level_struct_struct_map.csv",
            "${dataFilePath}/three_level_struct_struct_struct.parquet",
//                                    "${dataFilePath}/three_level_struct_struct_struct.orc",
//                                    "${dataFilePath}/three_level_struct_struct_struct.json",
            "${dataFilePath}/three_level_struct_struct_struct.csv"
    ]

    ffi = 0
    for (int ti = 18; ti < 27; ++ti) {
        // struct-array-array, struct-array-map, struct-array-struct,
        // struct-map-array, struct-map-map, struct-map-struct,
        // struct-struct-array, struct-struct-map, struct-struct-struct
        String table_name = table_names[ti]
        for (int fi = 0; fi < format_order.size(); ++fi) {
            String form = format_order[fi]
            sql "truncate table ${table_name};"
//            load_from_tvf(table_name, struct_files[ffi], form)
            load_from_s3(table_name, struct_files[ffi], form)
            ++ ffi
        }
        qt_select_count_struct """ select count() from ${table_name}; """
    }
    for (int i = 18; i < 27; ++i ) {
        // struct-array-array, struct-array-map, struct-array-struct,
        // struct-map-array, struct-map-map, struct-map-struct,
        // struct-struct-array, struct-struct-map, struct-struct-struct
        String table_name = table_names[i]
        // select element_at(column)

        order_qt_select_struct "select struct_element(${colNameArr[0]}, 1), struct_element(${colNameArr[0]}, 'col_1') from ${table_name} where k1 IS NOT NULL order by k1 limit 10;"
        // struct make error
        test {
            sql "select struct_element(${colNameArr[0]}, -1), struct_element(${colNameArr[0]}, 'not_exist') from ${table_name} order by k1 limit 10;"
            exception("the specified field index out of bound")
        }

        test {
            sql "select struct_element(${colNameArr[0]}, 0), struct_element(${colNameArr[0]}, 'not_exist') from ${table_name} order by k1 limit 10;"
            exception("the specified field index out of bound")
        }
        test {
            sql "select struct_element(${colNameArr[0]}, 1000) from ${table_name} order by k1 limit 10;"
            exception("the specified field index out of bound")
        }
        // select * from table where groupby|orderby column will meet exception
        groupby_or_orderby_exception(true, table_names[i], colNameArr[0])
        groupby_or_orderby_exception(false, table_names[i], colNameArr[0])

        // select * from table where groupby|orderby element_at(column)
        agg_expr = "struct_element(${colNameArr[0]}, 1)"
        groupby_or_orderby_exception(true, table_names[i], agg_expr)
        groupby_or_orderby_exception(false, table_names[i], agg_expr)
    }

    // most-nested-column
    // struct-array-array
    agg_expr = "struct_element(${colNameArr[0]}, 1)[1][1]"
    select_nested_scala_element_at(agg_expr, table_names[18])
    groupby_or_orderby_element_at(true, table_names[18], agg_expr)
    groupby_or_orderby_element_at(false, table_names[18], agg_expr)
    // struct-array-map
    agg_expr = "struct_element(${colNameArr[0]}, 1)[1][map_keys(struct_element(${colNameArr[0]}, 1)[1])[1]]"
    select_nested_scala_element_at(agg_expr, table_names[19])
    groupby_or_orderby_element_at(true, table_names[19], agg_expr)
    groupby_or_orderby_element_at(false, table_names[19], agg_expr)
    // struct-array-struct
    // select element_at(column)
    order_qt_select_struct "select struct_element(struct_element(${colNameArr[0]}, 1)[1], 1), struct_element(struct_element(${colNameArr[0]}, 1)[1],'col1') from ${table_names[20]} where k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where element_at(column) with equal expr
    order_qt_select_struct "select struct_element(struct_element(${colNameArr[0]}, 1)[1], 1) from ${table_names[20]} where struct_element(struct_element(${colNameArr[0]}, 1)[1], 1) IS NOT NULL AND k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where groupby|orderby column will meet exception
    groupby_or_orderby_exception(true, table_names[20], colNameArr[0])
    groupby_or_orderby_exception(false, table_names[20], colNameArr[0])
    // select * from table where groupby|orderby element_at(column)
    agg_expr = "struct_element(struct_element(${colNameArr[0]}, 1)[1], 1)"
    groupby_or_orderby_element_at(true, table_names[20], agg_expr)
    groupby_or_orderby_element_at(false, table_names[20], agg_expr)

    // struct-map-array
    agg_expr = "struct_element(${colNameArr[0]}, 1)[map_keys(struct_element(${colNameArr[0]}, 1))[1]][1]"
    select_nested_scala_element_at(agg_expr, table_names[21])
    groupby_or_orderby_element_at(true, table_names[21], agg_expr)
    groupby_or_orderby_element_at(false, table_names[21], agg_expr)
    // struct-map-map
    agg_expr = "struct_element(${colNameArr[0]}, 1)[map_keys(struct_element(${colNameArr[0]}, 1))[1]][map_keys(struct_element(${colNameArr[0]}, 1)[map_keys(struct_element(${colNameArr[0]}, 1))[1]])[1]]"
    select_nested_scala_element_at(agg_expr, table_names[22])
    groupby_or_orderby_element_at(true, table_names[22], agg_expr)
    groupby_or_orderby_element_at(false, table_names[22], agg_expr)
    // struct-map-struct
    // select element_at(column)
    // select struct_element(struct_element(c_bool, 1)[map_keys(struct_element(c_bool, 1))[1]], 'col1') from three_level_struct_map_struct where k1 IS NOT NULL order by k1 limit 10;
    order_qt_select_struct "select struct_element(struct_element(${colNameArr[0]}, 1)[map_keys(struct_element(${colNameArr[0]}, 1))[1]], 1), struct_element(struct_element(${colNameArr[0]}, 1)[map_keys(struct_element(${colNameArr[0]}, 1))[1]], 'col1') from ${table_names[23]} where k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where element_at(column) with equal expr
    order_qt_select_struct "select struct_element(struct_element(${colNameArr[0]}, 1)[map_keys(struct_element(${colNameArr[0]}, 1))[1]], 1) from ${table_names[23]} where struct_element(struct_element(${colNameArr[0]}, 1)[map_keys(struct_element(${colNameArr[0]}, 1))[1]], 1) IS NOT NULL AND k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where groupby|orderby column will meet exception
    groupby_or_orderby_exception(true, table_names[23], colNameArr[0])
    groupby_or_orderby_exception(false, table_names[23], colNameArr[0])
    // select * from table where groupby|orderby element_at(column)
    agg_expr = "struct_element(struct_element(${colNameArr[0]}, 1)[map_keys(struct_element(${colNameArr[0]}, 1))[1]], 1)"
    groupby_or_orderby_element_at(true, table_names[23], agg_expr)
    groupby_or_orderby_element_at(false, table_names[23], agg_expr)
    
    // struct-struct-array
    agg_expr = "struct_element(struct_element(${colNameArr[0]}, 1),1)[1]"
    select_nested_scala_element_at(agg_expr, table_names[24])
    groupby_or_orderby_element_at(true, table_names[24], agg_expr)
    groupby_or_orderby_element_at(false, table_names[24], agg_expr)
    // struct-struct-map
    agg_expr = "struct_element(struct_element(${colNameArr[0]}, 1),1)[map_keys(struct_element(struct_element(${colNameArr[0]}, 1),1))[1]]"
    select_nested_scala_element_at(agg_expr, table_names[25])
    groupby_or_orderby_element_at(true, table_names[25], agg_expr)
    groupby_or_orderby_element_at(false, table_names[25], agg_expr)
    // struct-struct-struct
    // select element_at(column)
    order_qt_select_struct "select struct_element(struct_element(struct_element(${colNameArr[0]}, 1),1),1), struct_element(struct_element(struct_element(${colNameArr[0]}, 1),1), 'col1') from ${table_names[26]} where k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where element_at(column) with equal expr
    order_qt_select_struct "select struct_element(struct_element(struct_element(${colNameArr[0]}, 1),1),1) from ${table_names[26]} where struct_element(struct_element(struct_element(${colNameArr[0]}, 1),1), 'col1') IS NOT NULL AND k1 IS NOT NULL order by k1 limit 10;"
    // select * from table where groupby|orderby column will meet exception
    groupby_or_orderby_exception(true, table_names[26], colNameArr[0])
    groupby_or_orderby_exception(false, table_names[26], colNameArr[0])
    // select * from table where groupby|orderby element_at(column)
    agg_expr = "struct_element(struct_element(struct_element(${colNameArr[0]}, 1),1),1)"
    groupby_or_orderby_element_at(true, table_names[26], agg_expr)
    groupby_or_orderby_element_at(false, table_names[26], agg_expr)
}
