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

suite("test_s3_tvf_from_arm", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    def outFilePath = "test_outfile_from_x86/outfile_"

    // csv
    qt_s3_tvf_from_arm_csv """ SELECT * FROM S3 (
        "uri" = "http://${bucket}.${s3_endpoint}/${outFilePath}*0.csv",
        "s3.access_key"= "${ak}",
        "s3.secret_key" = "${sk}",
        "format" = "csv",
        "region" = "${region}"
    );
    """

    // orc
    qt_s3_tvf_from_arm_orc """ SELECT * FROM S3 (
        "uri" = "http://${bucket}.${s3_endpoint}/${outFilePath}*0.orc",
        "s3.access_key"= "${ak}",
        "s3.secret_key" = "${sk}",
        "format" = "orc",
        "region" = "${region}"
    );
    """

    // parquet
    qt_s3_tvf_from_arm_parquet """ SELECT * FROM S3 (
        "uri" = "http://${bucket}.${s3_endpoint}/${outFilePath}*0.parquet",
        "s3.access_key"= "${ak}",
        "s3.secret_key" = "${sk}",
        "format" = "parquet",
        "region" = "${region}"
    );
    """
}