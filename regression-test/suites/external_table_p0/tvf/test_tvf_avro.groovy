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

    // def all_type_file = "all_type.avro";
    // def format = "avro"

    // // s3 config
    // String ak = getS3AK()
    // String sk = getS3SK()
    // String s3_endpoint = getS3Endpoint()
    // String region = getS3Region()
    // String bucket = context.config.otherConfigs.get("s3BucketName");
    // def s3Uri = "https://${bucket}.${s3_endpoint}/regression/datalake/pipeline_data/tvf/${all_type_file}";

    // // hdfs config
    // String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    // String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    // def hdfsUserName = "doris"
    // def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    // def hdfsUri = "${defaultFS}" + "/user/doris/preinstalled_data/avro/avro_all_types/${all_type_file}"

    // // TVF s3()
    // qt_1 """ 
    //         desc function s3(
    //         "uri" = "${s3Uri}",
    //         "ACCESS_KEY" = "${ak}",
    //         "SECRET_KEY" = "${sk}",
    //         "REGION" = "${region}",
    //         "FORMAT" = "${format}");
    //     """

    // qt_2 """
    //         select count(*) from s3(
    //         "uri" ="${s3Uri}",
    //         "ACCESS_KEY" = "${ak}",
    //         "SECRET_KEY" = "${sk}",
    //         "REGION" = "${region}",
    //         "provider" = "${getS3Provider()}",
    //         "FORMAT" = "${format}");
    //     """

    // order_qt_1 """
    //        select * from s3(
    //         "uri" = "${s3Uri}",
    //         "ACCESS_KEY" = "${ak}",
    //         "SECRET_KEY" = "${sk}",
    //         "REGION" = "${region}",
    //         "provider" = "${getS3Provider()}",
    //         "FORMAT" = "${format}");
    //     """

    // order_qt_2 """
    //        select anArray from s3(
    //         "uri" = "${s3Uri}",
    //         "ACCESS_KEY" = "${ak}",
    //         "SECRET_KEY" = "${sk}",
    //         "REGION" = "${region}",
    //         "provider" = "${getS3Provider()}",
    //         "FORMAT" = "${format}");
    //     """

    // order_qt_3 """
    //        select aMap from s3(
    //         "uri" = "${s3Uri}",
    //         "ACCESS_KEY" = "${ak}",
    //         "SECRET_KEY" = "${sk}",
    //         "REGION" = "${region}",
    //         "provider" = "${getS3Provider()}",
    //         "FORMAT" = "${format}");
    //     """

    // order_qt_4 """
    //        select anEnum from s3(
    //         "uri" = "${s3Uri}",
    //         "ACCESS_KEY" = "${ak}",
    //         "SECRET_KEY" = "${sk}",
    //         "REGION" = "${region}",
    //         "provider" = "${getS3Provider()}",
    //         "FORMAT" = "${format}");
    //     """

    // order_qt_5 """
    //        select aRecord from s3(
    //         "uri" = "${s3Uri}",
    //         "ACCESS_KEY" = "${ak}",
    //         "SECRET_KEY" = "${sk}",
    //         "REGION" = "${region}",
    //         "provider" = "${getS3Provider()}",
    //         "FORMAT" = "${format}");
    //     """

    // order_qt_6 """
    //        select aUnion from s3(
    //         "uri" = "${s3Uri}",
    //         "ACCESS_KEY" = "${ak}",
    //         "SECRET_KEY" = "${sk}",
    //         "REGION" = "${region}",
    //         "provider" = "${getS3Provider()}",
    //         "FORMAT" = "${format}");
    //     """

    // order_qt_7 """
    //        select mapArrayLong from s3(
    //         "uri" ="${s3Uri}",
    //         "ACCESS_KEY" = "${ak}",
    //         "SECRET_KEY" = "${sk}",
    //         "REGION" = "${region}",
    //         "provider" = "${getS3Provider()}",
    //         "FORMAT" = "${format}");
    //     """

    // order_qt_8 """
    //        select arrayMapBoolean from s3(
    //         "uri" = "${s3Uri}",
    //         "ACCESS_KEY" = "${ak}",
    //         "SECRET_KEY" = "${sk}",
    //         "REGION" = "${region}",
    //         "provider" = "${getS3Provider()}",
    //         "FORMAT" = "${format}");
    //     """

    // order_qt_10 """
    //        select arrayMapBoolean,aBoolean,aMap,aLong,aUnion from s3(
    //         "uri" = "${s3Uri}",
    //         "ACCESS_KEY" = "${ak}",
    //         "SECRET_KEY" = "${sk}",
    //         "REGION" = "${region}",
    //         "provider" = "${getS3Provider()}",
    //         "FORMAT" = "${format}");
    //     """

    // order_qt_11 """
    //        select aString,aDouble,anEnum from s3(
    //         "uri" = "${s3Uri}",
    //         "ACCESS_KEY" = "${ak}",
    //         "SECRET_KEY" = "${sk}",
    //         "REGION" = "${region}",
    //         "provider" = "${getS3Provider()}",
    //         "FORMAT" = "${format}");
    //     """

    // order_qt_12 """
    //        select aRecord,aMap,aFloat from s3(
    //         "uri" = "${s3Uri}",
    //         "ACCESS_KEY" = "${ak}",
    //         "SECRET_KEY" = "${sk}",
    //         "REGION" = "${region}",
    //         "provider" = "${getS3Provider()}",
    //         "FORMAT" = "${format}");
    //     """

    // // TVF hdfs()
    // String enabled = context.config.otherConfigs.get("enableHiveTest")
    // if (enabled != null && enabled.equalsIgnoreCase("true")) {
    //     try {
    //         qt_3 """ 
    //             desc function HDFS(
    //             "uri" = "${hdfsUri}",
    //             "fs.defaultFS" = "${defaultFS}",
    //             "hadoop.username" = "${hdfsUserName}",
    //             "FORMAT" = "${format}"); """

    //         order_qt_9 """ select * from HDFS(
    //                     "uri" = "${hdfsUri}",
    //                     "fs.defaultFS" = "${defaultFS}",
    //                     "hadoop.username" = "${hdfsUserName}",
    //                     "format" = "${format}")"""

    //         qt_4 """ select count(*) from HDFS(
    //                     "uri" = "${hdfsUri}",
    //                     "fs.defaultFS" = "${defaultFS}",
    //                     "hadoop.username" = "${hdfsUserName}",
    //                     "format" = "${format}"); """

    //         order_qt_13 """ select arrayMapBoolean,aBoolean,aMap,aLong,aUnion from HDFS(
    //                     "uri" = "${hdfsUri}",
    //                     "fs.defaultFS" = "${defaultFS}",
    //                     "hadoop.username" = "${hdfsUserName}",
    //                     "format" = "${format}")"""

    //         order_qt_14 """ select aString,aDouble,anEnum from HDFS(
    //                     "uri" = "${hdfsUri}",
    //                     "fs.defaultFS" = "${defaultFS}",
    //                     "hadoop.username" = "${hdfsUserName}",
    //                     "format" = "${format}")"""

    //         order_qt_15 """ select aRecord,aMap,aFloat from HDFS(
    //                     "uri" = "${hdfsUri}",
    //                     "fs.defaultFS" = "${defaultFS}",
    //                     "hadoop.username" = "${hdfsUserName}",
    //                     "format" = "${format}")"""
    //     } finally {
    //     }
    // }
}
