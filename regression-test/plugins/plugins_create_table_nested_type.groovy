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

// create table with nested data type, now default complex data include array, map, struct
Suite.metaClass.create_table_with_nested_type = { int max_depth, def type_arr, String tb_name /* param */ ->
    Suite suite = delegate as Suite

    try {
        if (type_arr.size() != max_depth) {
            throw new Exception("level not equal type_arr size")
        }
    } catch (Exception e) {
        logger.info(e.message)
        return
    }

    def cur_depth = type_arr.size()
    max_depth = max_depth > 9 ? 9 : max_depth
    max_depth = max_depth < 1 ? 1 : max_depth

//        def datatype_arr = ["boolean", "tinyint(4)", "smallint(6)", "int(11)", "bigint(20)", "largeint(40)", "float",
//                           "double", "decimal(20, 3)", "decimalv3(20, 3)", "date", "datetime", "datev2", "datetimev2(0)",
//                           "char(15)", "varchar(100)", "text", "hll","bitmap", "QUANTILE_STATE"]
    def datatype_arr = ["boolean", "tinyint(4)", "smallint(6)", "int(11)", "bigint(20)", "largeint(40)", "float",
                       "double", "decimal(20, 3)", "decimalv3(20, 3)", "date", "datetime", "datev2", "datetimev2(0)",
                       "char(15)", "varchar(100)", "text"]
    def col_name_arr = ["c_bool", "c_tinyint", "c_smallint", "c_int", "c_bigint", "c_largeint", "c_float",
                      "c_double", "c_decimal", "c_decimalv3", "c_date", "c_datetime", "c_datev2", "c_datetimev2",
                      "c_char", "c_varchar", "c_string"]
    def complex_datatype_arr = ["array", "map", "struct"]
    def base_struct_scala = "col1:int(11),col2:tinyint(4),col3:smallint(6),col4:boolean,col5:bigint(20),col6:largeint(40)," +
            "col7:float,col8:double,col9:decimal(20, 3),col10:decimalv3(20, 3),col11:date,col12:datetime,col13:datev2,col14:datetimev2(0)," +
            "col15:char(15),col16:varchar(100),col17:text"

    def colCount = 1

    def getDataType
    getDataType = { i, level ->

        StringBuilder res = new StringBuilder();
        def data_r = type_arr[cur_depth - level]

        if (level == 1) {
            if (data_r == 0) {
                res.append(complex_datatype_arr[data_r]+"<"+ datatype_arr[i] +">");
            } else if (data_r == 1) {
                if (i == 16) {
                    res.append(complex_datatype_arr[data_r]+"<"+ datatype_arr[i] +"," + datatype_arr[0] +">");
                } else if (i == 17 || i == 18 || i == 19) {
                    res.append(complex_datatype_arr[data_r]+"<int(11)," + datatype_arr[i] +">");
                } else {
                    res.append(complex_datatype_arr[data_r]+"<"+ datatype_arr[i] +"," + datatype_arr[i+1] +">");
                }
            } else if (data_r == 2) {
                res.append(complex_datatype_arr[data_r]+"<"+ base_struct_scala +">");
            }
        } else {
            level--;
            if (data_r == 0) {
                res.append(complex_datatype_arr[data_r]+"<"+getDataType(i, level)+">");
            } else if (data_r == 1) {
                if (i == 17 || i == 18 || i == 19) {
                    res.append(complex_datatype_arr[data_r]+"<int(11)," + getDataType(i, level) +">");
                } else {
                    res.append(complex_datatype_arr[data_r]+"<"+datatype_arr[i]+"," +getDataType(i, level)+">")
                }

            } else if (data_r == 2) {
                res.append(complex_datatype_arr[data_r]+"<col_"+colCount+":" + getDataType(i, level) +">");
                colCount++;
            }
        }
        return res.toString()
    }

    def stmt = "CREATE TABLE IF NOT EXISTS " + tb_name + "(\n" +
            "`k1` bigint(11) NULL,\n"

    for (int i = 0; i < datatype_arr.size(); i++) {
        String strTmp = "`" + col_name_arr[i] + "` " + getDataType(i, max_depth) + " NULL,\n";
        stmt += strTmp
    }

    stmt = stmt.substring(0, stmt.length()-2)
    stmt += ") ENGINE=OLAP\n" +
            "DUPLICATE KEY(`k1`)\n" +
            "COMMENT 'OLAP'\n" +
            "DISTRIBUTED BY HASH(`k1`) BUCKETS 10\n" +
            "PROPERTIES(\"replication_num\" = \"1\");"
    return stmt
}

logger.info("Added 'create_table_with_nested_type' function to Suite")

Suite.metaClass.get_create_table_with_nested_type { int depth, String tb_name ->

    List<List<Integer>> res = new ArrayList<>();
    List<Integer> track = new ArrayList();

//    int depth = 1;

    List<Integer> nums = new ArrayList<>([0, 1, 2]);

    def backtrack
    backtrack = {
        if (track.size() == depth) {
            List<Integer> copied = new ArrayList<>(track);
            res.add(copied);
            return;
        }

        for (int i = 0; i < nums.size(); i++) {
            track.add(nums.get(i));
            backtrack();
            track.remove(track.size() - 1);
        }
    }

    backtrack();
    for (int i = 0; i < res.size; i++) {
        def date_type_str = ""
        for (int j = 0; j < res[i].size; j++) {
            date_type_str += res[i][j] + " "
        }
        logger.info(date_type_str)
        def result = create_table_with_nested_type(depth, res[i], tb_name)
        logger.info(result)
    }
}

logger.info("Added 'get_create_table_with_nested_type' function to Suite")
