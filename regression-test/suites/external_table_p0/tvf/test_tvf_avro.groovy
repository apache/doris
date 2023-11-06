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

suite("test_tvf_avro", "external,hive,tvf,avro,external_docker") {

    def all_type_file = "all_type.avro";
    def format = "avro"

    // s3 config
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");
    def s3Uri = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/tvf/${all_type_file}";

    // hdfs config
    String hdfs_port = context.config.otherConfigs.get("hdfs_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def hdfsUserName = "doris"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def hdfsUri = "${defaultFS}" + "/user/doris/preinstalled_data/avro/avro_all_types/${all_type_file}"

    // TVF s3()
    qt_1 """ 
            desc function s3(
            "uri" = "${s3Uri}",
            "ACCESS_KEY" = "${ak}",
            "SECRET_KEY" = "${sk}",
            "REGION" = "${region}",
            "FORMAT" = "${format}"); 
        """

    qt_2 """
            select count(*) from s3(
            "uri" ="${s3Uri}",
            "ACCESS_KEY" = "${ak}",
            "SECRET_KEY" = "${sk}",
            "REGION" = "${region}",
            "FORMAT" = "${format}"); 
        """

    qt_3 """
           select * from s3(
            "uri" = "${s3Uri}",
            "ACCESS_KEY" = "${ak}",
            "SECRET_KEY" = "${sk}",
            "REGION" = "${region}",
            "FORMAT" = "${format}") order by aInt, aLong, aFloat; 
        """

    qt_4 """
           select anArray from s3(
            "uri" = "${s3Uri}",
            "ACCESS_KEY" = "${ak}",
            "SECRET_KEY" = "${sk}",
            "REGION" = "${region}",
            "FORMAT" = "${format}");
        """

    qt_5 """
           select aMap from s3(
            "uri" = "${s3Uri}",
            "ACCESS_KEY" = "${ak}",
            "SECRET_KEY" = "${sk}",
            "REGION" = "${region}",
            "FORMAT" = "${format}"); 
        """

    qt_6 """
           select anEnum from s3(
            "uri" = "${s3Uri}",
            "ACCESS_KEY" = "${ak}",
            "SECRET_KEY" = "${sk}",
            "REGION" = "${region}",
            "FORMAT" = "${format}");
        """

    qt_7 """
           select aRecord from s3(
            "uri" = "${s3Uri}",
            "ACCESS_KEY" = "${ak}",
            "SECRET_KEY" = "${sk}",
            "REGION" = "${region}",
            "FORMAT" = "${format}"); 
        """

    qt_8 """
           select aUnion from s3(
            "uri" = "${s3Uri}",
            "ACCESS_KEY" = "${ak}",
            "SECRET_KEY" = "${sk}",
            "REGION" = "${region}",
            "FORMAT" = "${format}"); 
        """

    qt_9 """
           select mapArrayLong from s3(
            "uri" ="${s3Uri}",
            "ACCESS_KEY" = "${ak}",
            "SECRET_KEY" = "${sk}",
            "REGION" = "${region}",
            "FORMAT" = "${format}"); 
        """

    qt_10 """
           select arrayMapBoolean from s3(
            "uri" = "${s3Uri}",
            "ACCESS_KEY" = "${ak}",
            "SECRET_KEY" = "${sk}",
            "REGION" = "${region}",
            "FORMAT" = "${format}"); 
        """

    // TVF hdfs()
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            qt_hdfs_1 """ 
                desc function HDFS(
                "uri" = "${hdfsUri}",
                "fs.defaultFS" = "${defaultFS}",
                "hadoop.username" = "${hdfsUserName}",
                "FORMAT" = "${format}"); """

            qt_hdfs_2 """ select * from HDFS(
                        "uri" = "${hdfsUri}",
                        "fs.defaultFS" = "${defaultFS}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "${format}") order by aInt, aLong, aFloat; """

            qt_hdfs_3 """ select count(*) from HDFS(
                        "uri" = "${hdfsUri}",
                        "fs.defaultFS" = "${defaultFS}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "${format}"); """
        } finally {
        }
    }
}