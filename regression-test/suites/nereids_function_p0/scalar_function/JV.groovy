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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("nereids_scalar_fn_JV") {

    sql "use regression_test_nereids_function_p0"
    
    // define a sql table from load
    def testTblList = [
//        "fn_test_json_variant_agg",
        "json_variant_rowstore_load_unique",
        "test_json_variant_load_duplicate",
        "test_json_variant_load_unique"
    ]

    // column name :
    //id               | bigint  | No   | true  | NULL    | AUTO_INCREMENT |
    //| kjson            | json    | Yes  | false | NULL    | NONE           |
    //| kvariant         | variant | Yes  | false | NULL    | NONE           |
    //| kjson_notnull    | json    | No   | false | NULL    | NONE           |
    //| kvariant_notnull | variant | No   | false | NULL    | NONE           |
    //+------------------+---------+------+-------+---------+----------------+
    def extract_res_func = { tbl, json_col, variant_col, extractPath ->
        // test jsonb_extract result with json_extract with element_at for variant
        // only support extract key-value
        def jsonb_extract_res = sql """ select jsonb_extract(${json_col}, '\\\$.${extractPath}') from ${tbl} where jsonb_extract(${json_col}, '\\\$.${extractPath}') is NOT NULL order by id; """
        def json_extract_res = sql """ select json_extract(${json_col}, '\\\$.${extractPath}') from ${tbl} where jsonb_extract(${json_col}, '\\\$.${extractPath}') is NOT NULL order by id; """
        // exam: object.strings -> variant_col['object']['strings']
        List<List<Object>> variant_extract_res
        if (extractPath.contains(".")) {
            def extractPathVariant = extractPath.replaceAll("\\.", "']['")
            extractPathVariant = "['${extractPathVariant}']"
            variant_extract_res = sql """ select ${variant_col}${extractPathVariant} from ${tbl} where jsonb_extract(${json_col}, '\\\$.${extractPath}') is NOT NULL order by id; """
        } else {
            variant_extract_res = sql """ select element_at(${variant_col}, '${extractPath}') from ${tbl} where jsonb_extract(${json_col}, '\\\$.${extractPath}') is NOT NULL order by id; """
        }

        // check the result of jsonb_extract, json_extract and element_at
        assertEquals(jsonb_extract_res.size(), json_extract_res.size())
        assertEquals(jsonb_extract_res.size(), variant_extract_res.size())
        for (def i = 0; i < jsonb_extract_res.size(); i++) {
            if (jsonb_extract_res[i][0] != json_extract_res[i][0]) {
                log.info("jsonb_extract_res[${i}][0]: ${jsonb_extract_res[i][0]} is not equal to json_extract_res[${i}][0]: ${json_extract_res[i][0]}")
            }
//            assertEquals(jsonb_extract_res[i][0], json_extract_res[i][0])
            if (jsonb_extract_res[i][0] != variant_extract_res[i][0] && jsonb_extract_res[i][0] != "\"" + variant_extract_res[i][0].toString() + "\"") {
                log.info("jsonb_extract_res[${i}][0]: ${jsonb_extract_res[i][0]} is not equal to variant_extract_res[${i}][0]: ${variant_extract_res[i][0]}")
            }
//            assertEquals(jsonb_extract_res[i][0], variant_extract_res[i][0])
        }
    }

    def checkAllNull = { sql_res ->
        for (def i = 1; i < sql_res.size(); i++) {
            if (sql_res[i][0] != null) {
                return false
            }
        }
        return true
    }
    def jsonb_extract_type_func = { tbl, json_col, extractPath, func_type ->
        def types = ["string", "int", "array","object"]
        for (def type : types) {
            def func_name = "jsonb_extract_${func_type}"
            def res = sql """ select ${func_name}(${json_col}, '${extractPath}') from ${tbl} where jsonb_type(${json_col}, '${extractPath}') == '${type}' order by id; """
            if (checkAllNull(res)) {
                log.info("func_name: ${func_name}(${json_col}, '${extractPath}') with filter type ${type} is all NULL")
            } else {
                qt_sql """ select ${func_name}(${json_col}, '${extractPath}') from ${tbl} where jsonb_type(${json_col}, '${extractPath}') == '${type}' AND ${func_name}(${json_col}, '${extractPath}') IS NOT NULL order by id; """
            }
        }
    }

    def jsonb_col_path_check_func = { tbl, json_col, extractPaths, func_names->
        // jsonb_exists_path
        for (def func_name : func_names) {
            for (def extractPath : extractPaths) {
                def null_res = sql """ select ${func_name}(${json_col}, '${extractPath}') from ${tbl} order by id; """
                if (checkAllNull(null_res)) {
                    log.info("func_name: ${func_name}(${json_col}, '${extractPath}') is all NULL")
                } else {
                    qt_sql """ select ${func_name}(${json_col}, '${extractPath}') from ${tbl} where ${func_name}(${json_col}, '${extractPath}') IS NOT NULL order by id; """
                }
            }
        }
    }

    for (def testTable : testTblList) {

        extract_res_func(testTable, "kjson_notnull", "kvariant", "strings")
        extract_res_func(testTable, "kjson_notnull", "kvariant", "numbers")
        extract_res_func(testTable, "kjson_notnull", "kvariant", "array")
        extract_res_func(testTable, "kjson_notnull", "kvariant", "object")
        extract_res_func(testTable, "kjson_notnull", "kvariant", "object.strings")
        extract_res_func(testTable, "kjson_notnull", "kvariant", "object.numbers")
        extract_res_func(testTable, "kjson_notnull", "kvariant", "object.array")
        extract_res_func(testTable, "kjson_notnull", "kvariant", "object.object")

        // extract some array json
        // json_extract with array json path: json_extract(kjson_notnull, '$.[0]') vs jsonb_extract(kjson_notnull, '$[0]')
        def json_extract_arr = sql """ select jsonb_extract(kjson_notnull, '\\\$[0]'), json_extract(kjson_notnull, '\\\$.[0]') from ${testTable} where jsonb_type(kjson_notnull, '\\\$') == 'array' order by id; """
        // check the result of jsonb_extract and json_extract
        for (def i = 1; i < json_extract_arr.size(); i++) {
            if (json_extract_arr[i][0] != json_extract_arr[i][1]) {
                log.info("jsonb_extract_res[${i}][0]: ${json_extract_arr[i][0]} is not equal to json_extract_res[${i}][0]: ${json_extract_arr[i][1]}")
            }
//            assertEquals(json_extract_arr[i][0], json_extract_arr[i][1], "row ${i}")
        }

        // use cast with variant
        // NOTE: just use cast(kvariant as array<string>) will be NULL which should be resolved.
        def variant_arr = sql """select cast(cast(kvariant as string) as array<string>)[1] from ${testTable} where jsonb_type(kjson_notnull, '\\\$') == 'array' order by id;"""
        assertEquals(json_extract_arr.size(), variant_arr.size())
        for (def i = 1; i < json_extract_arr.size(); i++) {
            if (json_extract_arr[i][0] != variant_arr[i][0]) {
                log.info("jsonb_extract_res[${i}][0]: ${json_extract_arr[i][0]} is not equal to variant_arr[${i}][0]: ${variant_arr[i][0]}")
            }
//            assertEquals(json_extract_arr[i][0], variant_arr[i][0], "row ${i}")
        }

        // jsonb_extract_multipath
        qt_jsonb_extract_multipath "SELECT id, jsonb_extract(kjson_notnull, '\\\$', '\\\$.*'), json_extract(kjson_notnull, '\\\$', '\\\$.*')  FROM ${testTable} ORDER BY id"

        // jsonb_extract_string
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$", "string")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$[0]", "string")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.strings", "string")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.array", "string")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.number", "string")

        // jsonb_extract_int
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$", "int")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$[0]", "int")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.strings", "int")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.array", "int")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.number", "int")

        // jsonb_extract_bigint
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$", "bigint")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$[0]", "bigint")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.strings", "bigint")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.array", "bigint")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.number", "bigint")

        // josnb_extract_largeint
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$", "largeint")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$[0]", "largeint")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.strings", "largeint")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.array", "largeint")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.number", "largeint")

        // jsonb_extract_double
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$", "double")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$[0]", "double")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.strings", "double")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.array", "double")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.number", "double")

        // jsonb_extract_bool
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$", "bool")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$[0]", "bool")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.strings", "bool")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.array", "bool")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.number", "bool")

        // jsonb_extract_isnull
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$", "isnull")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$[0]", "isnull")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.strings", "isnull")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.array", "isnull")
        jsonb_extract_type_func(testTable, "kjson_notnull", "\\\$.object.number", "isnull")

        // jsonb other funcs
        def extractaPaths = [
                "\\\$",
                "\\\$.object.strings",
                "\\\$.object.array",
                "\\\$.object.number",
                "\\\$.object.strings[0]",
                "\\\$.object.array[0]",
                "\\\$.object.number[0]",
                "\\\$[0]",
                "\\\$[1]",
                "\\\$[-1]",
                "\\\$[-10]",
                "\\\$[last]",
                "\\\$[last-0]",
                "\\\$[last-1]",
                "\\\$.object.array[last]",
                "\\\$.object.array[last-0]",
                "\\\$.object.array[last-1]",
                "\\\$.object.array[-1]",
                "\\\$.object.array[-10]"
        ]
        def func_names = ["jsonb_exists_path", "jsonb_type", "json_keys"]
        jsonb_col_path_check_func(testTable, "kjson_notnull", extractaPaths, func_names)


    }


}
