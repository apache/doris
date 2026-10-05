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

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_decimal256_outfile_csv") {
    sql "set enable_nereids_planner = true;"
    sql "set enable_decimal256 = true;"

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    sql "DROP TABLE IF EXISTS `test_decimal256_outfile_csv`"
    sql """
    CREATE TABLE IF NOT EXISTS `test_decimal256_outfile_csv` (
      `k1` decimalv3(76, 9) NULL COMMENT "",
      `k2` decimalv3(76, 10) NULL default '999999999999999999999999999999999999999999999999999999999999999999.9999999999' COMMENT "",
      `k3` decimalv3(76, 11) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
   streamLoad {
       // you can skip db declaration, because a default db has already been
       // specified in ${DORIS_HOME}/conf/regression-conf.groovy
       // db 'regression_test'
       table "test_decimal256_outfile_csv"

       // default label is UUID:
       // set 'label' UUID.randomUUID().toString()

       // default column_separator is specify in doris fe config, usually is '\t'.
       // this line change to ','
       set 'column_separator', ','

       // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
       // also, you can stream load a http stream, e.g. http://xxx/some.csv
       file """test_decimal256_load.csv"""

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
           assertEquals(19, json.NumberTotalRows)
           assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
           assertTrue(json.LoadBytes > 0)
       }
   }
    sql "sync"
    qt_sql_select_all """
        SELECT * FROM test_decimal256_outfile_csv t order by 1,2,3;
    """


    def outFilePath = "${bucket}/outfile/csv/test_decimal256_outfile_csv/exp_"

    def outfile_to_S3 = { export_table_name, foramt ->
        // select ... into outfile ...
        def res = sql """
            SELECT * FROM ${export_table_name} t ORDER BY k1
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS ${foramt}
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """
        return res[0][3]
    }

    try {
        logger.info("outfile: " + outFilePath)
        def outfile_url = outfile_to_S3("test_decimal256_outfile_csv", "csv")

        qt_select_tvf1 """ SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}"
                        );
                        """
    } finally {
    }
}