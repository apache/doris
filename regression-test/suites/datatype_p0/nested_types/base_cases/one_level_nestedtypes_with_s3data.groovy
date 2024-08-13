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

suite("one_level_nestedtypes_with_s3data") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def dataFilePath = "https://"+"${bucket}"+"."+"${s3_endpoint}"+"/regression/datalake"
//    def dataFilePath = "/mnt/disk1/wangqiannan/export/ol"
    def table_names = ["test_array_one_level", "test_map_one_level", "test_struct_one_level"]

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

    def groupby_or_orderby_element_at = {is_groupby, table_name, agg_expr ->
        if (is_groupby) {
            order_qt_sql "select ${agg_expr} from ${table_name} where k1 IS NOT NULL group by ${agg_expr};"
        } else {
            order_qt_sql "select ${agg_expr} from ${table_name} where k1 IS NOT NULL order by ${agg_expr} limit 10;"
        }
    }

    def be_id = 10139
    def load_from_tvf = {table_name, uri_file, format ->
        if (format == "csv") {
            order_qt_sql_tvf """select * from local(
                "file_path" = "${uri_file}",
                "backend_id" = "${be_id}",
                "column_separator"="|",
                "format" = "${format}") order by c1 limit 10; """

            sql """
            insert into ${table_name} select * from local(
            "file_path" = "${uri_file}",
            "backend_id" = "${be_id}",
            "column_separator"="|",
            "format" = "${format}"); """
        } else {
            order_qt_sql_tvf """select * from local(
                "file_path" = "${uri_file}",
                "backend_id" = "${be_id}",
                "column_separator"="|",
                "format" = "${format}") order by k1 limit 10; """

            sql """
            insert into ${table_name} select * from local(
            "file_path" = "${uri_file}",
            "backend_id" = "${be_id}",
            "column_separator"="|",
            "format" = "${format}"); """
        }
        // where to filter different format data
        qt_select_doris """ select c_char from ${table_name} where k1 IS NOT NULL order by k1 limit 10; """
    }
    def load_from_s3 = {table_name, uri_file, format ->
        if (format == "csv") {
            order_qt_sql_s3 """select * from s3(
                "uri" = "${uri_file}",
                    "s3.access_key"= "${ak}",
                    "s3.secret_key" = "${sk}",
                    "format" = "${format}",
                    "provider" = "${getS3Provider()}",
                    "column_separator"="|",
                    "read_json_by_line"="true") order by c1 limit 10; """

            sql """
            insert into ${table_name} select * from s3(
            "uri" = "${uri_file}",
                    "s3.access_key"= "${ak}",
                    "s3.secret_key" = "${sk}",
                    "format" = "${format}",
                    "column_separator"="|",
                    "provider" = "${getS3Provider()}",
                    "read_json_by_line"="true"); """
        } else {
            order_qt_sql_s3 """select * from s3(
                "uri" = "${uri_file}",
                    "s3.access_key"= "${ak}",
                    "s3.secret_key" = "${sk}",
                    "format" = "${format}",
                    "provider" = "${getS3Provider()}",
                    "read_json_by_line"="true") order by k1 limit 10; """

            sql """
            insert into ${table_name} select * from s3(
            "uri" = "${uri_file}",
                    "s3.access_key"= "${ak}",
                    "s3.secret_key" = "${sk}",
                    "format" = "${format}",
                    "provider" = "${getS3Provider()}",
                    "read_json_by_line"="true"); """
        }
        // where to filter different format data
        order_qt_select_doris """ select * from ${table_name} where k1 IS NOT NULL order by k1 limit 10; """
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
            "parquet", "orc",
//            "json",
            "csv"]
    // create tables
    for (int i = 0; i < table_names.size(); ++i) {
        sql """ DROP TABLE IF EXISTS ${table_names[i]} """
        String result = create_table_with_nested_type(1, [i], table_names[i])
        sql result
    }

    //========================= ARRAY =========================
    // insert into doris table
    ArrayList<String> array_files = [
                                    "${dataFilePath}/one_level_array.parquet",
                                    "${dataFilePath}/one_level_array.orc",
//                                    "${dataFilePath}/one_level_array.json",
                                    "${dataFilePath}/one_level_array.csv"
    ]
    int fi = 0
    for (String f : array_files) {
        sql "truncate table ${table_names[0]};"
//        load_from_tvf(table_names[0], f, format_order[fi])
        load_from_s3(table_names[0], f, format_order[fi])
        ++ fi
    }
    // select element_at(column)
    for (String col : colNameArr) {
        // first
        order_qt_select_arr "select ${col}[1] from ${table_names[0]} where k1 IS NOT NULL order by k1 limit 10;"
        // last
        order_qt_select_arr "select ${col}[-1] from ${table_names[0]} where k1 IS NOT NULL order by k1 limit 10;"
        // null
        order_qt_select_arr_null "select ${col}[0] from ${table_names[0]} where k1 IS NOT NULL order by k1 limit 10;"
        // null
        order_qt_select_arr_null "select ${col}[1000] from ${table_names[0]} where k1 IS NOT NULL order by k1 limit 10;"
    }
    // select * from table where element_at(column) with equal expr
    for (String col : colNameArr) {
        order_qt_select_arr "select ${col}[1], ${col}[-1] from ${table_names[0]} where k1 IS NOT NULL AND ${col}[1]<${col}[-1] order by k1 limit 10;"
    }
    // select * from table where groupby|orderby column will meet exception
    for (String col : colNameArr) {
        groupby_or_orderby_exception(true, table_names[0], col)
        groupby_or_orderby_exception(false, table_names[0], col)
    }
    // select * from table where groupby|orderby element_at(column)
    for (String col : colNameArr) {
        String agg_expr = "${col}[1]"
        groupby_or_orderby_element_at(true, table_names[0], agg_expr)
        groupby_or_orderby_element_at(false, table_names[0], agg_expr)
    }

    //========================== MAP ==========================
    // insert into doris table
    ArrayList<String> map_files = [
                                    "${dataFilePath}/one_level_map.parquet",
                                    "${dataFilePath}/one_level_map.orc",
//                                    "${dataFilePath}/one_level_map.json",
                                    "${dataFilePath}/one_level_map.csv"
    ]
    fi = 0
    for (String f : map_files) {
        sql "truncate table ${table_names[1]};"
//        load_from_tvf(table_names[1], f, format_order[fi])
        load_from_s3(table_names[1], f, format_order[fi])
        ++ fi
    }
    // select element_at(column)
    for (String col : colNameArr) {
        // first
        order_qt_select_map "select ${col}[map_keys(${col})[1]] from ${table_names[1]} where k1 IS NOT NULL order by k1 limit 10;"
        // last
        order_qt_select_map "select ${col}[map_keys(${col})[-1]] from ${table_names[1]} where k1 IS NOT NULL order by k1 limit 10;"
        // null::q:q
        order_qt_select_map_null "select ${col}[map_keys(${col})[0]] from ${table_names[1]} where k1 IS NOT NULL order by k1 limit 10;"
        // null
        order_qt_select_map_null "select ${col}[map_keys(${col})[1000]] from ${table_names[1]} where k1 IS NOT NULL order by k1 limit 10;"
    }
    // select * from table where element_at(column) with equal expr
    for (String col : colNameArr) {
        order_qt_select_map "select ${col}[map_keys(${col})[1]], ${col}[map_keys(${col})[-1]] from ${table_names[1]} where ${col}[map_keys(${col})[1]]<${col}[map_keys(${col})[-1]] AND k1 IS NOT NULL order by k1 limit 10;"
    }
    // select * from table where groupby|orderby column will meet exception
    for (String col : colNameArr) {
        groupby_or_orderby_exception(true, table_names[1], col)
        groupby_or_orderby_exception(false, table_names[1], col)
    }
    // select * from table where groupby|orderby element_at(column)
    for (String col : colNameArr) {
        String agg_expr = "${col}[map_keys(${col})[1]]"
        groupby_or_orderby_element_at(true, table_names[1], agg_expr)
        groupby_or_orderby_element_at(false, table_names[1], agg_expr)
    }

    //========================= STRUCT ========================
    // insert into doris table
    ArrayList<String> struct_files = [
                                    "${dataFilePath}/one_level_struct.parquet",
                                    "${dataFilePath}/one_level_struct.orc",
//                                    "${dataFilePath}/one_level_struct.json",
                                    "${dataFilePath}/one_level_struct.csv"
    ]
    fi = 0
    for (String f : struct_files) {
        sql "truncate table ${table_names[2]};"
//        load_from_tvf(table_names[2], f, format_order[fi])
        load_from_s3(table_names[2], f, format_order[fi])
        ++ fi
    }
    // select element_at(column)
    // first
    order_qt_select_struct "select struct_element(${colNameArr[0]}, 1), struct_element(${colNameArr[0]}, 'col1') from ${table_names[2]} where k1 IS NOT NULL order by k1 limit 10;"
    // last
    order_qt_select_struct "select struct_element(${colNameArr[0]}, 17), struct_element(${colNameArr[0]}, 'col17') from ${table_names[2]} where k1 IS NOT NULL order by k1 limit 10;"
    // struct make error
    test {
        sql "select struct_element(${colNameArr[0]}, -1), struct_element(${colNameArr[0]}, 'not_exist') from ${table_names[2]} order by k1;"
        exception("the specified field index out of bound")
    }

    test {
        sql "select struct_element(${colNameArr[0]}, 0), struct_element(${colNameArr[0]}, 'not_exist') from ${table_names[2]} order by k1;"
        exception("the specified field index out of bound")
    }
    test {
        sql "select struct_element(${colNameArr[0]}, 1000) from ${table_names[2]} order by k1;"
        exception("the specified field index out of bound")
    }

    String res = sql "select struct_element(${colNameArr[0]}, 17) from ${table_names[2]} order by k1 limit 1;"
    // select * from table where element_at(column) with equal expr
    order_qt_select_struct "select * from ${table_names[2]} where struct_element(${colNameArr[0]}, 1) > 100 AND k1 IS NOT NULL order by k1 limit 10;"
    order_qt_select_struct "select * from ${table_names[2]} where struct_element(${colNameArr[0]}, 'col17') = '${res}' AND k1 IS NOT NULL order by k1 limit 10;"

    // select * from table where groupby|orderby column will meet exception

    groupby_or_orderby_exception(true, table_names[2], colNameArr[0])
    groupby_or_orderby_exception(false, table_names[2], colNameArr[0])

    // select * from table where groupby|orderby element_at(column)
    String agg_expr = "struct_element(${colNameArr[0]}, 1)"
    groupby_or_orderby_element_at(true, table_names[2], agg_expr)
    groupby_or_orderby_element_at(false, table_names[2], agg_expr)

}
