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

suite("test_csv_schema_with_path_partition_keys", "p0,external") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3Endpoint = getS3Endpoint()
    String bucket = context.config.otherConfigs.get("s3BucketName")

    order_qt_csv_schema_with_path_partition_keys """
        select c1, c2, dt1 from
        s3(
            "URI" = "https://${bucket}.${s3Endpoint}/regression/tvf/test_path_partition_keys/dt1=hello/c.csv",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "FORMAT" = "csv",
            "column_separator" = ",",
            "csv_schema" = "c1:int;c2:string",
            "use_path_style" = "false", -- aliyun does not support path_style
            "path_partition_keys" = "dt1") order by c1, c2;
    """

    test {
        sql """
            select * from
            s3(
                "URI" = "https://${bucket}.${s3Endpoint}/regression/tvf/test_path_partition_keys/dt1=hello/c.csv",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "FORMAT" = "csv",
                "column_separator" = ",",
                "csv_schema" = "c1:int;c2:string",
                "use_path_style" = "false", -- aliyun does not support path_style
                "path_partition_keys" = "C1");
        """
        exception "Path partition column conflicts with an existing column: C1"
    }
}
