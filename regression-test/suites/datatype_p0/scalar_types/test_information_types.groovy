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

suite("test_information_types") {

    def tb_name = "test_information_schema_types"

    def datatype_arr = ["boolean", "tinyint(4)", "smallint(6)", "int(11)", "bigint(20)", "largeint(40)", "float",
                       "double", "decimal(20, 3)", "decimalv3(20, 3)", "date", "datetime", "datev2", "datetimev2(0)",
                       "char(15)", "varchar(100)", "text", "ipv4", "ipv6", "variant"]
    def col_name_arr = ["c_bool", "c_tinyint", "c_smallint", "c_int", "c_bigint", "c_largeint", "c_float",
                      "c_double", "c_decimal", "c_decimalv3", "c_date", "c_datetime", "c_datev2", "c_datetimev2",
                      "c_char", "c_varchar", "c_string", "c_ipv4", "c_ipv6", "c_variant"]

    def stmt = "CREATE TABLE IF NOT EXISTS " + tb_name + "(\n" +
                "`k1` bigint(11) NULL,\n"

        for (int i = 0; i < datatype_arr.size(); i++) {
            String strTmp = "`" + col_name_arr[i] + "` " + datatype_arr[i] + " NULL,\n";
            stmt += strTmp
        }

        stmt = stmt.substring(0, stmt.length()-2)
        stmt += ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT 'OLAP'\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 10\n" +
                "PROPERTIES(\"replication_num\" = \"1\");"

    // check column type and data type is "unknown" or not
    sql stmt;
    def res = sql "select COLUMN_TYPE, DATA_TYPE from information_schema.columns where table_name = '" + tb_name + "'";
    for (int i = 0; i < res.size(); i++) {
        log.info("column_type: " + res[i][0] + ", data_type: " + res[i][1]);
        assert(res[i][0] != "unknown");
        assert(res[i][1] != "unknown");
    }

}
