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

suite("test_broker_load", "p0") {

    def tables = ["part",
                  "upper_case",
                  "reverse",
                  "set1",
                  "set2",
                  "set3",
                  "set4",
                  "set5",
                  "set6",
                  "set7",
                  "null_default",
                  "filter",
                  "path_column",
                  "parquet_s3_case1", // col1 not in file but in table, will load default value for it.
                  "parquet_s3_case2", // x1 not in file, not in table, will throw "col not found" error.
                  "parquet_s3_case3", // p_comment not in table but in file, load normally.
                  "parquet_s3_case4", // all tables are in table but not in file, will throw "no column found" error.
                  "parquet_s3_case5", // x1 not in file, not in table, will throw "col not found" error.
                  "parquet_s3_case6", // normal
                  "parquet_s3_case7", // col5 will be ignored, load normally
                  "parquet_s3_case8", // first column in table is not specified, will load default value for it.
                  "parquet_s3_case9", // first column in table is not specified, will load default value for it.
                 ]
    def paths = ["s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/path/*/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/part*",
                 "s3://doris-build-hk-1308700295/regression/load/data/random_all_types/part*",
    ]
    def columns_list = ["""p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                   """p_partkey, p_name, p_size""",
                   """p_partkey""",
                   """p_partkey""",
                   """p_partkey,  p_size""",
                   """p_partkey""",
                   """p_partkey,  p_size""",
                   """p_partkey,  p_size""",
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, col1""",
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, x1""",
                   """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment""",
                   """col1, col2, col3, col4""",
                   """p_partkey, p_name, p_mfgr, x1""",
                   """p_partkey, p_name, p_mfgr, p_brand""",
                   """p_partkey, p_name, p_mfgr, p_brand""",
                   """p_name, p_mfgr""",
                   """"""
                   ]
    def column_in_paths = ["", "", "", "", "", "", "", "", "", "", "", "", "COLUMNS FROM PATH AS (city)", "", "", "", "", "", "", "", "", ""]
    def preceding_filters = ["", "", "", "", "", "", "", "", "", "", "", "preceding filter p_size < 10", "", "", "", "", "", "", "", "", "", ""]
    def set_values = ["",
                      "",
                      "SET(comment=p_comment, retailprice=p_retailprice, container=p_container, size=p_size, type=p_type, brand=p_brand, mfgr=p_mfgr, name=p_name, partkey=p_partkey)",
                      "set(p_name=upper(p_name),p_greatest=greatest(cast(p_partkey as int), cast(p_size as int)))",
                      "set(p_partkey = p_partkey + 100)",
                      "set(partkey = p_partkey + 100)",
                      "set(partkey = p_partkey + p_size)",
                      "set(tmpk = p_partkey + 1, partkey = tmpk*2)",
                      "set(partkey = p_partkey + 1, partsize = p_size*2)",
                      "set(partsize = p_partkey + p_size)",
                      "",
                      "",
                      "",
                      "",
                      "",
                      "",
                      "",
                      "set(col4 = x1)",
                      "set(col4 = p_brand)",
                      "set(col5 = p_brand)",
                      "",
                      ""
    ]
    def where_exprs = ["", "", "", "", "", "", "", "", "", "", "", "where p_partkey>10", "", "", "", "", "", "", "", "", "", ""]

    def etl_info = ["unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "unselected.rows=163706; dpp.abnorm.ALL=0; dpp.norm.ALL=36294",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "\\N",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "\\N",
                    "\\N",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=200000",
                    "unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=4096"
                    ]

    def error_msg = ["",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "type:LOAD_RUN_FAIL; msg:errCode = 2, detailMessage = failed to find default value expr for slot: x1",
                    "",
                    "type:LOAD_RUN_FAIL; msg:errCode = 2, detailMessage = failed to init reader for file s3://doris-build-hk-1308700295/regression/load/data/part-00000-cb9099f7-a053-4f9a-80af-c659cfa947cc-c000.snappy.parquet, err: No columns found in parquet file",
                    "type:LOAD_RUN_FAIL; msg:errCode = 2, detailMessage = failed to find default value expr for slot: x1",
                    "",
                    "",
                    "",
                    ""
                    ]

    String ak = getS3AK()
    String sk = getS3SK()
    String enabled = context.config.otherConfigs.get("enableBrokerLoad")

    def do_load_job = {uuid, path, table, columns, column_in_path, preceding_filter,
                          set_value, where_expr ->
        String columns_str = ("$columns" != "") ? "($columns)" : "";
        sql """
            LOAD LABEL $uuid (
                DATA INFILE("$path")
                INTO TABLE $table
                FORMAT AS "PARQUET"
                $columns_str
                $column_in_path
                $preceding_filter
                $set_value
                $where_expr
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "cos.ap-hongkong.myqcloud.com",
                "AWS_REGION" = "ap-hongkong"
            );
            """
        logger.info("Submit load with lable: $uuid, table: $table, path: $path")
    }

    def set_be_config = { flag->
        String[][] backends = sql """ show backends; """
        assertTrue(backends.size() > 0)
        for (String[] backend in backends) {
            // No need to set this config anymore, but leave this code sample here
            // StringBuilder setConfigCommand = new StringBuilder();
            // setConfigCommand.append("curl -X POST http://")
            // setConfigCommand.append(backend[2])
            // setConfigCommand.append(":")
            // setConfigCommand.append(backend[5])
            // setConfigCommand.append("/api/update_config?")
            // String command1 = setConfigCommand.toString() + "enable_new_load_scan_node=$flag"
            // logger.info(command1)
            // def process1 = command1.execute()
            // int code = process1.waitFor()
            // assertEquals(code, 0)
        }
    }

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def uuids = []
        sql """ADMIN SET FRONTEND CONFIG ("enable_new_load_scan_node" = "true");"""
        set_be_config.call('true')
        try {
            i = 0
            for (String table in tables) {
                sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${table}_create.sql""").text

                def uuid = UUID.randomUUID().toString().replace("-", "0")
                uuids.add(uuid)
                do_load_job.call(uuid, paths[i], table, columns_list[i], column_in_paths[i], preceding_filters[i],
                        set_values[i], where_exprs[i])
                i++
            }

            i = 0
            for (String label in uuids) {
                max_try_milli_secs = 600000
                while (max_try_milli_secs > 0) {
                    String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
                    if (result[0][2].equals("FINISHED")) {
                        logger.info("Load FINISHED " + label)
                        assertTrue(etl_info[i] == result[0][5], "expected: " + etl_info[i] + ", actual: " + result[0][5])
                        break;
                    }
                    if (result[0][2].equals("CANCELLED")) {
                        assertTrue(error_msg[i] == result[0][7], "expected: " + error_msg[i] + ", actual: " + result[0][7])
                        break;
                    }
                    Thread.sleep(1000)
                    max_try_milli_secs -= 1000
                    if(max_try_milli_secs <= 0) {
                        assertTrue(1 == 2, "Load Timeout.")
                    }
                }
                i++
            }

            order_qt_parquet_s3_case1 """select count(*) from parquet_s3_case1 where col1=10"""
            order_qt_parquet_s3_case3 """select count(*) from parquet_s3_case3 where p_partkey < 100000"""
            order_qt_parquet_s3_case6 """select count(*) from parquet_s3_case6 where p_partkey < 100000"""
            order_qt_parquet_s3_case7 """select count(*) from parquet_s3_case7 where col4=4"""
            order_qt_parquet_s3_case8 """ select count(*) from parquet_s3_case8 where p_partkey=1"""
            order_qt_parquet_s3_case9 """ select * from parquet_s3_case9"""

        } finally {
            sql """ADMIN SET FRONTEND CONFIG ("enable_new_load_scan_node" = "false");"""
            set_be_config.call('false')
            for (String table in tables) {
                sql new File("""${context.file.parent}/ddl/${table}_drop.sql""").text
            }
        }
    }
}

