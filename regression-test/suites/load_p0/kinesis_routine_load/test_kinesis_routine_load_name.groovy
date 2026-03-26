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

suite("test_kinesis_routine_load_name") {
    String awsRegion = context.config.otherConfigs.get("awsRegion")
    String awsAccessKey = context.config.otherConfigs.get("awsAccessKey")
    String awsSecretKey = context.config.otherConfigs.get("awsSecretKey")

    try {
        sql """
            CREATE ROUTINE LOAD test_kinesis_routine_load_name_too_much_filler_filler_filler_filler_filler_filler_filler
            COLUMNS TERMINATED BY "|"
            PROPERTIES (
                "format" = "csv",
                "max_batch_interval" = "5",
                "max_batch_rows" = "300000"
            )
            FROM KINESIS (
                "aws.region" = "${awsRegion}",
                "aws.access_key" = "${awsAccessKey}",
                "aws.secret_key" = "${awsSecretKey}",
                "kinesis_stream" = "invalid_stream",
                "property.kinesis_default_pos" = "TRIM_HORIZON"
            )
        """
    } catch (Exception e) {
        log.info("exception: ${e.toString()}".toString())
        assertEquals(e.toString().contains("Incorrect ROUTINE LOAD NAME name"), true)
        assertEquals(e.toString().contains("required format is"), true)
        assertEquals(e.toString().contains("Maybe routine load job name is longer than 64 or contains illegal characters"), true)
    }

    try {
        sql """
            CREATE ROUTINE LOAD x.y.z
            COLUMNS TERMINATED BY "|"
            PROPERTIES (
                "format" = "csv",
                "max_batch_interval" = "5"
            )
            FROM KINESIS (
                "aws.region" = "${awsRegion}",
                "aws.access_key" = "${awsAccessKey}",
                "aws.secret_key" = "${awsSecretKey}",
                "kinesis_stream" = "invalid_stream",
                "property.kinesis_default_pos" = "TRIM_HORIZON"
            )
        """
    } catch (Exception e) {
        log.info("exception: ${e.toString()}".toString())
        assertEquals(e.toString().contains("labelParts in load should be [db.]label"), true)
    }
}
