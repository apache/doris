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

suite("test_json_reader_without_object", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    sql """ set enable_nereids_timeout=false; """
    sql """ set max_scan_key_num = 48 """
    sql """ set max_pushdown_conditions_per_column=1024 """

    def dataFilePath = "https://"+"${bucket}"+"."+"${s3_endpoint}"+"/regression/jsondata"
    def dataSimpleNumber = "json_reader_without_object.json"
    def dataSimpleArray = "json_reader_without_object_array.json"

    // select expect error
    test {
        sql """ select * from s3(
                                "uri" = "${dataFilePath}/${dataSimpleNumber}",
                                    "s3.access_key"= "${ak}",
                                    "s3.secret_key" = "${sk}",
                                    "format" = "json",
                                    "provider" = "${getS3Provider()}",
                                    "read_json_by_line"="true"); """
        exception "DATA_QUALITY_ERROR"
    }

    test {
        sql """ select * from s3(
                                "uri" = "${dataFilePath}/${dataSimpleArray}",
                                    "s3.access_key"= "${ak}",
                                    "s3.secret_key" = "${sk}",
                                    "format" = "json",
                                    "provider" = "${getS3Provider()}",
                                    "strip_outer_array" = "true",
                                    "read_json_by_line"="true"); """
        exception "DATA_QUALITY_ERROR"
    }
}
