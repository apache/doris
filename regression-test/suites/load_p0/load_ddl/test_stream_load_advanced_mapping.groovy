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

suite("test_stream_load_advanced_mapping", "p0") {
    def tableName = "agg_tbl_basic"
    
    InetSocketAddress address = context.config.feHttpInetSocketAddress
    String user = context.config.feHttpUser
    String password = context.config.feHttpPassword
    String db = context.config.getDbNameByFile(context.file)

    // 创建测试表
    sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
    sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

    //step1: mapping all columns
    try {
        streamLoad {
            table tableName
            set 'column_separator', '|'
            file "full_columns_data.csv"
            set 'exec_mem_limit', '1'
            set 'columns', "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0)"
            time 10000
            
            check { result, exception, startTime, endTime ->
                assertTrue(exception == null)
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }
    } finally {
        sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
    }

    // step2: use default values and basic func
    try {
        sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text
        streamLoad {
            table tableName
            set 'column_separator', '|'
            set 'columns', 'k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19=to_bitmap("1,2,3"),k20=hll_hash("k00"),k21=TO_quantile_state(k04,0.5),kd12=CURRENT_TIMESTAMP(),kd14=NOW(),kd19=to_bitmap("4,5,6"),kd20=hll_hash("k00"),kd21=TO_quantile_state(k05,1.0)'
            file "full_columns_data.csv" 
            time 10000
            
            check { result, exception, startTime, endTime ->
                assertTrue(exception == null)
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }
    } finally {
        sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
    }
        

    // step3: set calculated value 
    try {
        sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text
        streamLoad {
            table tableName
            set 'columns', 'k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd04=k00 * 1,kd11=NOW(),kd13=date_add(kd11, interval 30 day),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0)'
            set 'column_separator', '|'
            file "full_columns_data.csv"  
            time 10000
            
            check { result, exception, startTime, endTime ->
                assertTrue(exception == null)
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }
        
    } finally {
        sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
    }

    // step4: set complex func
    try {
        sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text
        streamLoad {
            table tableName
            set 'columns', 'k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd11=str_to_date("2023-08-10", "%Y-%m-%d"),kd12=str_to_date(concat("2023-08-", random(5,10), " 12:00:00"), "%Y-%m-%d %H:%i:%s"),kd19=to_bitmap(concat(k00+2, ",", k00+3)),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0)'
            set 'column_separator', '|'
            file "full_columns_data.csv"  
            time 10000
            
            check { result, exception, startTime, endTime ->
                assertTrue(exception == null)
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }
        
    } finally {
        sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
    }


    // step5: s3 load with basic mapping default value
    def label = UUID.randomUUID().toString().replace("-", "0")
    def path = "s3://${getS3BucketName()}/regression/load/data/basic_data.csv"
    def format_str = "CSV"
    def ak = getS3AK()
    def sk = getS3SK()
    def seq_column = "K12"
    try {
        sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text
        def sql_str = """
                LOAD LABEL $label (
                    DATA INFILE("$path")
                    INTO TABLE $tableName
                    COLUMNS TERMINATED BY "|"
                    FORMAT AS $format_str
                    (k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)
                    SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))
                )
                WITH S3 (
                    "AWS_ACCESS_KEY" = "$ak",
                    "AWS_SECRET_KEY" = "$sk",
                    "AWS_ENDPOINT" = "${getS3Endpoint()}",
                    "AWS_REGION" = "${getS3Region()}",
                    "PROVIDER" = "${getS3Provider()}"
                )
                """
        logger.info("submit sql: ${sql_str}");
        sql """${sql_str}"""
        logger.info("Submit load with lable: $label, table: $tableName, path: $path")

        def max_try_milli_secs = 600000
        while (max_try_milli_secs > 0) {
            String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
            if (result[0][2].equals("FINISHED")) {
                logger.info("Load FINISHED " + label)
                break
            }
            if (result[0][2].equals("CANCELLED")) {
                def reason = result[0][7]
                logger.info("load failed, reason:$reason")
                assertTrue(1 == 2)
                break
            }
            Thread.sleep(1000)
            max_try_milli_secs -= 1000
            if(max_try_milli_secs <= 0) {
                assertTrue(1 == 2, "load Timeout: $label")
            }
        }
    } finally {
        sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
    }

    // step6: s3 load default values and basic func
    try {
        def label_1 = UUID.randomUUID().toString().replace("-", "0")
        sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text
        def sql_str = """
                LOAD LABEL $label_1 (
                    DATA INFILE("$path")
                    INTO TABLE $tableName
                    COLUMNS TERMINATED BY "|"
                    FORMAT AS $format_str
                    (k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)
                    SET (k19=to_bitmap("1,2,3"),k20=hll_hash("k00"),k21=TO_quantile_state(k04,0.5),kd12=CURRENT_TIMESTAMP(),kd14=NOW(),kd19=to_bitmap("4,5,6"),kd20=hll_hash("k00"),kd21=TO_quantile_state(k05,1.0))
                )
                WITH S3 (
                    "AWS_ACCESS_KEY" = "$ak",
                    "AWS_SECRET_KEY" = "$sk",
                    "AWS_ENDPOINT" = "${getS3Endpoint()}",
                    "AWS_REGION" = "${getS3Region()}",
                    "PROVIDER" = "${getS3Provider()}"
                )
                """
        logger.info("submit sql: ${sql_str}");
        sql """${sql_str}"""
        logger.info("Submit load with lable: $label, table: $tableName, path: $path")

        def max_try_milli_secs = 600000
        while (max_try_milli_secs > 0) {
            String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
            if (result[0][2].equals("FINISHED")) {
                logger.info("Load FINISHED " + label)
                break
            }
            if (result[0][2].equals("CANCELLED")) {
                def reason = result[0][7]
                logger.info("load failed, reason:$reason")
                assertTrue(1 == 2)
                break
            }
            Thread.sleep(1000)
            max_try_milli_secs -= 1000
            if(max_try_milli_secs <= 0) {
                assertTrue(1 == 2, "load Timeout: $label")
            }
        }
    } finally {
        sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
    }

    // step7: s3 set calculated value 
    try {
        def label_2 = UUID.randomUUID().toString().replace("-", "0")
        sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text
        def sql_str = """
                LOAD LABEL $label_2 (
                    DATA INFILE("$path")
                    INTO TABLE $tableName
                    COLUMNS TERMINATED BY "|"
                    FORMAT AS $format_str
                    (k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)
                    SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd04=k00 * 1,kd11=NOW(),kd13=date_add(kd11, interval 30 day),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))
                )
                WITH S3 (
                    "AWS_ACCESS_KEY" = "$ak",
                    "AWS_SECRET_KEY" = "$sk",
                    "AWS_ENDPOINT" = "${getS3Endpoint()}",
                    "AWS_REGION" = "${getS3Region()}",
                    "PROVIDER" = "${getS3Provider()}"
                )
                """
        logger.info("submit sql: ${sql_str}");
        sql """${sql_str}"""
        logger.info("Submit load with lable: $label, table: $tableName, path: $path")

        def max_try_milli_secs = 600000
        while (max_try_milli_secs > 0) {
            String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
            if (result[0][2].equals("FINISHED")) {
                logger.info("Load FINISHED " + label)
                break
            }
            if (result[0][2].equals("CANCELLED")) {
                def reason = result[0][7]
                logger.info("load failed, reason:$reason")
                assertTrue(1 == 2)
                break
            }
            Thread.sleep(1000)
            max_try_milli_secs -= 1000
            if(max_try_milli_secs <= 0) {
                assertTrue(1 == 2, "load Timeout: $label")
            }
        }
    } finally {
        sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
    }

    // step8: s3 set calculated value 
    try {
        def label_3 = UUID.randomUUID().toString().replace("-", "0")
        sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text
        def sql_str = """
                LOAD LABEL $label_3 (
                    DATA INFILE("$path")
                    INTO TABLE $tableName
                    COLUMNS TERMINATED BY "|"
                    FORMAT AS $format_str
                    (k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)
                    SET (k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd11=str_to_date("2023-08-10", "%Y-%m-%d"),kd12=str_to_date(concat("2023-08-", random(5,10), " 12:00:00"), "%Y-%m-%d %H:%i:%s"),kd19=to_bitmap(concat(k00+2, ",", k00+3)),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0))
                )
                WITH S3 (
                    "AWS_ACCESS_KEY" = "$ak",
                    "AWS_SECRET_KEY" = "$sk",
                    "AWS_ENDPOINT" = "${getS3Endpoint()}",
                    "AWS_REGION" = "${getS3Region()}",
                    "PROVIDER" = "${getS3Provider()}"
                )
                """
        logger.info("submit sql: ${sql_str}");
        sql """${sql_str}"""
        logger.info("Submit load with lable: $label, table: $tableName, path: $path")

        def max_try_milli_secs = 600000
        while (max_try_milli_secs > 0) {
            String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
            if (result[0][2].equals("FINISHED")) {
                logger.info("Load FINISHED " + label)
                break
            }
            if (result[0][2].equals("CANCELLED")) {
                def reason = result[0][7]
                logger.info("load failed, reason:$reason")
                assertTrue(1 == 2)
                break
            }
            Thread.sleep(1000)
            max_try_milli_secs -= 1000
            if(max_try_milli_secs <= 0) {
                assertTrue(1 == 2, "load Timeout: $label")
            }
        }
    } finally {
        sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
    }

}