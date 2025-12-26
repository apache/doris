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

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_load_ddl", "p0") {    
    InetSocketAddress address = context.config.feHttpInetSocketAddress
    String user = context.config.feHttpUser
    String password = context.config.feHttpPassword
    String db = context.config.getDbNameByFile(context.file)

    def tableName_1 = "test_load_ddl_unavaliable_column"

    //step1: mapping all columns
    try {
        // 创建测试表
        sql new File("""${context.file.parent}/ddl/${tableName_1}_drop.sql""").text
        sql new File("""${context.file.parent}/ddl/${tableName_1}_create.sql""").text
        streamLoad {
            table tableName_1
            set 'column_separator', '|'
            file "test_load_ddl_unavaliable_column.csv"
            set 'exec_mem_limit', '1'
            set 'columns', "k00=c01,k01,k02,k03,k04"
            time 10000
            
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("[ANALYSIS_ERROR]TStatus"))
                assertTrue(json.Message.contains("Unknown column 'c01' in 'table list' in PROJECT clause"))
            }
        }
    } finally {
        sql new File("""${context.file.parent}/ddl/${tableName_1}_drop.sql""").text
    }


    //step2: mapping hidden columns
    def tableName_2 = "test_load_ddl_hidden_column"
    sql "set show_hidden_columns = true"
    try {
        // 创建测试表
        sql new File("""${context.file.parent}/ddl/${tableName_2}_drop.sql""").text
        sql new File("""${context.file.parent}/ddl/${tableName_2}_create.sql""").text
        streamLoad {
            table tableName_2
            set 'column_separator', '|'
            file "test_load_ddl_hidden_column.csv"
            set 'exec_mem_limit', '1'
            set 'columns', "k00,k01,k02,k03,__DORIS_DELETE_SIGN__,__DORIS_VERSION_COL__,__DORIS_SEQUENCE_COL__"
            time 10000
            
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
             }
        }
    } finally {
        sql new File("""${context.file.parent}/ddl/${tableName_2}_drop.sql""").text
    }

    //step2: mapping hidden columns
    sql "set show_hidden_columns = true"
    try {
        // 创建测试表
        sql new File("""${context.file.parent}/ddl/${tableName_2}_drop.sql""").text
        sql new File("""${context.file.parent}/ddl/${tableName_2}_create.sql""").text
        streamLoad {
            table tableName_2
            set 'column_separator', '|'
            file "test_load_ddl_hidden_column.csv"
            set 'exec_mem_limit', '1'
            set 'columns', "k00,k01,k02,k03,__DORIS_DELETE_SIGN__,__DORIS_VERSION_COL__,__DORIS_SEQUENCE_COL__"
            time 10000
            
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
             }
        }
    } finally {
        sql new File("""${context.file.parent}/ddl/${tableName_2}_drop.sql""").text
    }

    //step3: mapping hidden columns
    sql "set show_hidden_columns = true"
    try {
        // 创建测试表
        sql new File("""${context.file.parent}/ddl/${tableName_2}_drop.sql""").text
        sql new File("""${context.file.parent}/ddl/${tableName_2}_create.sql""").text
        streamLoad {
            table tableName_2
            set 'column_separator', '|'
            file "test_load_ddl_hidden_column_with_func.csv"
            set 'exec_mem_limit', '1'
            set 'columns', "k00,k01,k02,k03,__DORIS_DELETE_SIGN__=to_bitmap(k03),__DORIS_VERSION_COL__= k00 * 100,__DORIS_SEQUENCE_COL__=57"
            time 10000
            
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("[ANALYSIS_ERROR]TStatus"))
                assertTrue(json.Message.contains("can not cast from origin type BITMAP to target type=TINYINT"))
             }
        }
    } finally {
        sql new File("""${context.file.parent}/ddl/${tableName_2}_drop.sql""").text
    }

    //step4: mapping hidden columns with err func
    sql "set show_hidden_columns = true"
    try {
        // 创建测试表
        sql new File("""${context.file.parent}/ddl/${tableName_2}_drop.sql""").text
        sql new File("""${context.file.parent}/ddl/${tableName_2}_create.sql""").text
        streamLoad {
            table tableName_2
            set 'column_separator', '|'
            file "test_load_ddl_hidden_column_with_func.csv"
            set 'exec_mem_limit', '1'
            set 'columns', "k00,k01,k02,k03,__DORIS_DELETE_SIGN__=HLL_HASH(k03),__DORIS_VERSION_COL__= k00 * 100,__DORIS_SEQUENCE_COL__=57"
            time 10000
            
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("[ANALYSIS_ERROR]TStatus"))
                assertTrue(json.Message.contains("can not cast from origin type HLL to target type=TINYINT"))
             }
        }
    } finally {
        sql new File("""${context.file.parent}/ddl/${tableName_2}_drop.sql""").text
    }

    //step5: mapping hidden columns with TO_QUANTILE_STATE func
    sql "set show_hidden_columns = true"
    try {
        // 创建测试表
        sql new File("""${context.file.parent}/ddl/${tableName_2}_drop.sql""").text
        sql new File("""${context.file.parent}/ddl/${tableName_2}_create.sql""").text
        streamLoad {
            table tableName_2
            set 'column_separator', '|'
            file "test_load_ddl_hidden_column_with_func.csv"
            set 'exec_mem_limit', '1'
            set 'columns', "k00,k01,k02,k03,__DORIS_DELETE_SIGN__=TO_QUANTILE_STATE(k03,1.0),__DORIS_VERSION_COL__= k00 * 100,__DORIS_SEQUENCE_COL__=57"
            time 10000
            
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("[ANALYSIS_ERROR]TStatus"))
                assertTrue(json.Message.contains("can not cast from origin type QUANTILE_STATE to target type=TINYINT"))
             }
        }
    } finally {
        sql new File("""${context.file.parent}/ddl/${tableName_2}_drop.sql""").text
    }




}