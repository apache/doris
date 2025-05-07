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

import com.google.common.base.Strings;

suite("test_tvf_with_role") {
    if (Strings.isNullOrEmpty(context.config.regressionAwsRoleArn)) {
        logger.info("skip ${name} case, because awsRoleArn is null or empty")
        return
    }


    if (Strings.isNullOrEmpty(context.config.awsRoleArn)) {
        logger.info("skip ${name} case, because awsRoleArn is null or empty")
        return
    }

    def endpoint = context.config.regressionAwsEndpoint
    def region = context.config.regressionAwsRegion
    def bucket = context.config.regressionAwsBucket
    def roleArn = context.config.regressionAwsRoleArn
    def externalId = context.config.regressionAwsExternalId

    sql """
        select count(*) from s3("uri" = "s3://${bucket}/regression/tpch/sf0.01/customer.csv.gz",
                    "s3.endpoint" = "${endpoint}",
                    "s3.region" = "${region}",
                    "s3.role_arn"= "${roleArn}",
                    "s3.external_id" = "${externalId}",
                    "format" = "csv",
                    "compress_type" = "gz",
                    "column_separator" = "|",
                    "use_path_style" = "false");
        """
}