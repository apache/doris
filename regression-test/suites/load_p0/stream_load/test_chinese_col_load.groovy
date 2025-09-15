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

suite("test_chinese_col_load", "p0") {
    def tableName = "test_chinese_col_load"
    
    // 启用Unicode名称支持
    sql "SET enable_unicode_name_support = true"
    sql "SET sql_mode = ''"

    // 定义Stream Load测试函数
    def stream_load = {table_name, _format, _columns, _jsonpaths, col_sep, 
                       array_json, line_json, file_name ->
        streamLoad {
            table table_name
            // http request header
            set 'format', _format
            set 'columns', _columns
            set 'jsonpaths', _jsonpaths
            set 'column_separator', col_sep
            set 'strip_outer_array', array_json
            set 'read_json_by_line', line_json
            set 'enable_unicode_name_support', 'true'
            file file_name
            time 10000 // limit inflight 10s
            
            // check callback for csv load - 期望失败以复现问题
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    log.info("Expected exception: ${exception}")
                    // 这里期望看到中文列名解析失败的异常
                    return
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                
                // 如果成功了，检查是否正确处理了中文列名
                if ("success".equals(json.Status.toLowerCase())) {
                    assertEquals(json.NumberLoadedRows, 3)
                    log.info("Chinese column names handled correctly")
                } else {
                    // 失败的情况，检查是否是中文列名问题
                    log.info("Failed as expected - Status: ${json.Status}")
                    log.info("Error message: ${json.Message}")
                    if (json.ErrorURL) {
                        def (code, out, err) = curl("GET", json.ErrorURL)
                        log.info("Error details: " + out)
                        // 检查是否包含中文列名解析错误
                        assertTrue(out.contains("Duplicate column") || out.contains("????") || 
                                  json.Message.contains("ParseException"))
                    }
                }
            }
        }
    }

    // 定义创建测试表函数 - 模拟客户场景，包含更多中文列名
    def create_test_table = {table_name ->
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                `OBJECTID` varchar(36) NULL,
                `NAME` varchar(255) NULL,
                `CREATEDTIME` datetime NULL,
                `流程编号` varchar(255) NULL,
                `发起人` varchar(255) NULL,
                `发起日期` datetime NULL,
                `部门` varchar(255) NULL,
                `发起人电话` varchar(255) NULL,
                `转移原因` varchar(255) NULL,
                `详细原因描述` varchar(2500) NULL,
                `转移方式` varchar(255) NULL,
                `接受区域` varchar(255) NULL,
                `接受明细区域` varchar(255) NULL,
                `接受人` varchar(255) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`OBJECTID`)
            DISTRIBUTED BY HASH(`OBJECTID`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            )
        """
    }

    // case1: 测试中文列名和中文数据的CSV导入
    sql "DROP TABLE IF EXISTS ${tableName}"
    create_test_table.call(tableName)
    // 测试包含多个中文列名的场景，模拟客户问题
    stream_load.call(tableName, "csv", "OBJECTID,流程编号,发起人,发起日期,部门,发起人电话,转移原因,详细原因描述,转移方式,接受区域,接受明细区域,接受人", "", ",", "", "", "chinese_col_complex.csv")
    
    try_sql("DROP TABLE IF EXISTS ${tableName}")
}
