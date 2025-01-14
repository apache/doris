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

suite("test_hdfs_parquet_group4","external,hive,tvf,external_docker") {
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def hdfsUserName = "doris"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def uri = ""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-02c2ced7-756d-4b85-a1cd-bd6651e89dfb-c000.snappy.parquet"
            order_qt_test_0 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-7a66d694-3b49-4588-97f8-e219a1c20133.c000.snappy.parquet"
            order_qt_test_1 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-7ea9d557-8828-4df3-bb78-928f47c623f7.c000.snappy.parquet"
            order_qt_test_2 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-15e6800a-1995-4089-8212-a563edf37f5b-c000.snappy.parquet"
            order_qt_test_3 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-63c2205d-84a3-4a66-bd7c-f69f5af55bbc.c000.snappy.parquet"
            order_qt_test_4 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-c707739f-45c0-4722-9837-4b11599ea66d.c000.snappy.parquet"
            order_qt_test_5 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-a186173b-63f7-4c84-b9e9-ab9eb682df75.c000.snappy.parquet"
            order_qt_test_6 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-951068bd-bcf4-4094-bb94-536f3c41d31f.c000.snappy.parquet"
            order_qt_test_7 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-1e2bdd3f-bc1e-4a3f-a535-34ab2e7306b0.c000.snappy.parquet"
            order_qt_test_8 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/00000000000000000001.checkpoint.0000000001.0000000001.59e66709-d0ca-402d-9375-0e3c4fcf8d73.parquet"
            order_qt_test_9 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-23a788af-acb6-434e-a9c9-aa15eefadab9-c000.snappy.parquet"
            order_qt_test_10 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00002-00e0efad-b3d9-4d70-ba55-f1d4c83cd675.c000.snappy.parquet"
            order_qt_test_11 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-ba4afeda-e581-4193-879d-12f07682e4d1-c000.snappy.parquet"
            order_qt_test_12 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-39aadeb3-8017-49b0-97cd-ed068caee353-c000.snappy.parquet"
            order_qt_test_13 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00003-5033ee91-1e9e-4399-a64d-8580656f35a7.c000.snappy.parquet"
            order_qt_test_14 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-0ca99241-8aa7-4f33-981b-e6fd611fe062-c000.snappy.parquet"
            order_qt_test_15 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-92af646f-8d26-483e-a7ba-2db048432fe6.c000.snappy.parquet"
            order_qt_test_16 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-46bd4dbe-c23b-4b4c-be4b-a7b6084a0c04-c000.snappy.parquet"
            order_qt_test_17 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-132cb407-0f83-4b06-9dc6-639f23baba79-c000.snappy.parquet"
            order_qt_test_18 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-b54dbea5-ad33-4bb9-93e4-889dbbf666ce-c000.snappy.parquet"
            order_qt_test_19 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-28393939-5f62-4205-99fa-f7a989a197d3.c000.snappy.parquet"
            order_qt_test_20 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-46308b3a-d718-438a-8a14-0460afb08d5d-c000.snappy.parquet"
            order_qt_test_21 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-2e2af0f0-bb19-4d64-99d8-ca5a949f3186-c000.snappy.parquet"
            order_qt_test_22 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-c4be8f9c-ffe0-4286-9904-62170d344825-c000.snappy.parquet"
            order_qt_test_23 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/byte_stream_split_float_and_double.parquet"
            order_qt_test_24 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-eebaa9a7-1a21-4e28-806c-24f24f8a0353-c000.snappy.parquet"
            order_qt_test_25 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/00000000000000000001.checkpoint.0000000001.0000000001.03288d7e-af16-44ed-829c-196064a71812.parquet"
            order_qt_test_26 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-a3937c44-1b9f-4398-8a6b-08aa0174e7f8-c000.snappy.parquet"
            order_qt_test_27 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-bbf9df63-ac4d-439e-b0a2-3e8a0e35c436-c000.snappy.parquet"
            order_qt_test_28 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-22a101a1-8f09-425e-847e-cbbe4f894eea.c000.snappy.parquet"
            order_qt_test_29 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-290f0f26-19cf-4772-821e-36d55d9b7872.c000.snappy.parquet"
            order_qt_test_30 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/871677fb-e0e3-46f8-9cc1-fe497e317216-0_0-28-26_20211216071453747.parquet"
            order_qt_test_31 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-68d36678-1302-4d91-a23d-1049d2630b60-c000.snappy.parquet"
            order_qt_test_32 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-dcb29d13-eeca-4fa6-a8bf-860da0131a5c.c000.snappy.parquet"
            order_qt_test_33 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-a875d565-492b-4423-90fe-b5aa285804aa.c000.snappy.parquet"
            order_qt_test_34 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-9542caf8-bad7-4cd5-9621-4e756b6767d7-c000.snappy.parquet"
            order_qt_test_35 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/data.parquet"
            order_qt_test_36 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-fa0de0b8-72ed-4d45-83d0-047de7094767-c000.snappy.parquet"
            order_qt_test_37 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-6622a76a-3107-4f21-9bc7-bece21e24aef-c000.snappy.parquet"
            order_qt_test_38 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00011-ebb931c8-4ae0-4320-a3a4-e4fcd91cef97-c000.snappy.parquet"
            order_qt_test_39 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/issue-5483.parquet"
            order_qt_test_40 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-81626978-0b68-49fe-8ce7-0869145ad4fe-c000.snappy.parquet"
            order_qt_test_41 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-cd9819d1-1410-45d9-82e6-b2c35aea723f-c000.snappy.parquet"
            order_qt_test_42 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-a947b0c2-6aa8-4da7-8ecf-81bc04c7ec9b.c000.snappy.parquet"
            order_qt_test_43 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/20230701_050241_00000_yfcmj-a9a22770-6b17-458d-995f-b15e5cce5d67.parquet"
            order_qt_test_44 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00002-d4a07e68-e1c3-4120-ab64-d5bf0a9a8130-c000.snappy.parquet"
            order_qt_test_45 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-be853604-1a95-499c-8bd3-0817e117e934-c000.snappy.parquet"
            order_qt_test_46 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-ab613d48-052b-4de3-916a-ee0d89139446.c000.snappy.parquet"
            order_qt_test_47 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-b53021d6-4e47-486c-8cf2-90b47361d1cf-c000.snappy.parquet"
            order_qt_test_48 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-e0b4887e-95f6-4ce1-b96c-32c5cf472476.c000.snappy.parquet"
            order_qt_test_49 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-fe628d69-2a2a-490c-9115-9971777168de-c000.snappy.parquet"
            order_qt_test_50 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-4d4a6969-dfec-42a7-b8f8-ae38a5e81775-c000.snappy.parquet"
            order_qt_test_51 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-41f747ba-fbdf-4129-b4dc-0b9d6c3dc588-c000.snappy.parquet"
            order_qt_test_52 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00018-14a227ec-b0e7-4be5-8144-58faed7450b0-c000.snappy.parquet"
            order_qt_test_53 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-35886fe5-5a75-4085-b6e3-b77e265f0b69.c000.snappy.parquet"
            order_qt_test_54 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-4ae6c4f5-a00a-4d72-8e19-8fb58e5374ec-c000.snappy.parquet"
            order_qt_test_55 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-ce9d03a6-039f-43ef-955f-2843c7f6d7ea-c000.snappy.parquet"
            order_qt_test_56 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-3181df7e-a84a-41a3-b42a-83f522a83a03.c000.snappy.parquet"
            order_qt_test_57 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-20a863e0-890d-4776-8825-f9dccc8973ba.c000.snappy.parquet"
            order_qt_test_58 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-e90e48ef-2b52-4d32-be2f-9051e90920ce.c000.snappy.parquet"
            order_qt_test_59 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-30d20302-2223-4370-b4ec-9129845af85d-c000.snappy.parquet"
            order_qt_test_60 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-6ccf825d-46ea-47be-973c-b3c31ade63da-c000.snappy.parquet"
            order_qt_test_61 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-0aa47759-3062-4e53-94c8-2e20a0796fee-c000.snappy.parquet"
            order_qt_test_62 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-5e65e887-f7f9-495e-a496-be0555925f40.c000.snappy.parquet"
            order_qt_test_63 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/issue-10873.parquet"
            order_qt_test_64 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/old-repeated-int.parquet"
            order_qt_test_65 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-8f003f1f-2c6e-4a0b-a848-3f7e2ab1aa02-c000.snappy.parquet"
            order_qt_test_66 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-39ae9f09-271c-485b-be11-367f4b8bed6c.c000.snappy.parquet"
            order_qt_test_67 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-e15d8e8c-2e34-4d76-bacf-6315428daba0-c000.snappy.parquet"
            order_qt_test_68 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-55d13068-dd09-43fd-ad1a-42835e2befa6-c000.snappy.parquet"
            order_qt_test_69 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-0d620652-b8a8-4265-b819-3d9dede05cf3.c000.snappy.parquet"
            order_qt_test_70 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00002-62ca7e3f-2a91-498a-b99e-266e6d720982-c000.snappy.parquet"
            order_qt_test_71 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-63d2167d-76a7-46d5-9717-66226902de9d.c000.snappy.parquet"
            order_qt_test_72 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-14be2f63-39c1-4279-9476-3329295d8b69-c000.snappy.parquet"
            order_qt_test_73 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-946b010f-db20-4fde-860e-a0d90082de75-c000.snappy.parquet"
            order_qt_test_74 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-570d8e52-652d-4892-8bdc-7fa5466ffa69.c000.snappy.parquet"
            order_qt_test_75 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/old-repeated-string.parquet"
            order_qt_test_76 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-596d8885-4c76-4436-89ff-6a60cbf497fb-c000.snappy.parquet"
            order_qt_test_77 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-7750833b-13f2-46b9-8f5e-30019fcdc0d2.c000.snappy.parquet"
            order_qt_test_78 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-85005a78-494c-430e-a326-2dd9e5313eaa.c000.snappy.parquet"
            order_qt_test_79 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/00000000000000000001.checkpoint.156b3304-76b2-49c3-a9a1-626f07df27c9.parquet"
            order_qt_test_80 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-e8015e28-8049-4ce0-99d2-c8d291e4eac9-c000.snappy.parquet"
            order_qt_test_81 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-3e38bdb2-fccc-4bd0-8f60-4ce39aa50ef9-c000.snappy.parquet"
            order_qt_test_82 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-3880d75f-8df1-4f4d-9cdb-717f53353af2-c000.snappy.parquet"
            order_qt_test_83 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-f9e92025-e4a9-4fbe-89c5-b3d88a2ca720-c000.snappy.parquet"
            order_qt_test_84 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-f6243f45-fea9-4d0e-8744-ef23cfe2b99c.c000.snappy.parquet"
            order_qt_test_85 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-bca61531-f38a-497a-9862-10b978a7ab0b.c000.snappy.parquet"
            order_qt_test_86 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-fecae78c-38f8-438a-bc05-451ea338c0bd.c000.snappy.parquet"
            order_qt_test_87 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-fad997ee-643c-4ce6-b554-3e1d596857ba-c000.snappy.parquet"
            order_qt_test_88 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-383111b3-5cc1-4fca-9bd2-759433f7754f.c000.snappy.parquet"
            order_qt_test_89 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-aeede0e4-d6fd-4425-ab3e-be80963fe765-c000.snappy.parquet"
            order_qt_test_90 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-44c24d68-b78c-492c-883e-0102c3713fbf-c000.snappy.parquet"
            order_qt_test_91 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-672c4212-0279-4b4d-88b7-0437c28c9135.c000.snappy.parquet"
            order_qt_test_92 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00003-2cab4d4e-6e68-4e68-9d5e-c29468593af0.c000.snappy.parquet"
            order_qt_test_93 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-0d1f5755-cdde-49da-803c-1ad8f995b8da-c000.snappy.parquet"
            order_qt_test_94 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00009-5aec8cfb-6be5-4560-b0dd-ad89910c3ffd-c000.snappy.parquet"
            order_qt_test_95 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-fb0e719f-6805-4839-91fb-918e1b003d7b.c000.snappy.parquet"
            order_qt_test_96 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-383afb1a-87de-4e70-86ab-c21ae44c7f3f.c000.snappy.parquet"
            order_qt_test_97 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00002-c86c22bb-9076-4ab4-94cf-d38b51cae032-c000.snappy.parquet"
            order_qt_test_98 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-2de3ebdf-cd8a-47fe-93e4-6f6d4bcf49df.c000.snappy.parquet"
            order_qt_test_99 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-9f31b17e-3bb8-45e6-b88a-30eed293bfa0.c000.snappy.parquet"
            order_qt_test_100 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-efd26171-4ebf-410d-b556-6e18f11acc42.c000.snappy.parquet"
            order_qt_test_101 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-ddc6c400-a412-4d79-a5ed-e319e98d33ba-c000.snappy.parquet"
            order_qt_test_102 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00003-347dd4d3-f31e-40bc-9467-864f421208c7.c000.snappy.parquet"
            order_qt_test_103 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-50743692-12e7-4ffd-8bdd-f666f620be72-c000.snappy.parquet"
            order_qt_test_104 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-9cfa2c9c-efae-4e0a-bf25-eba86bfdce11.c000.snappy.parquet"
            order_qt_test_105 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-e9233d4b-dd6e-479c-9eef-889b8264bfb1.c000.snappy.parquet"
            order_qt_test_106 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-90827a60-5339-4b09-a428-2c3d692736db-c000.snappy.parquet"
            order_qt_test_107 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-4b336398-8b08-4697-a40a-687504cbfdce.c000.snappy.parquet"
            order_qt_test_108 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-274dcbf4-d64c-43ea-8eb7-e153feac98ce-c000.snappy.parquet"
            order_qt_test_109 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-04200a92-bc84-4c35-80ea-910906481a4f-c000.snappy.parquet"
            order_qt_test_110 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00005-bc21ce9f-2122-4582-ae11-06bf40a5e5ee-c000.snappy.parquet"
            order_qt_test_111 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-d5bf7aab-81d2-466b-b4b7-b9fb56e1742f-c000.snappy.parquet"
            order_qt_test_112 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-8929e1cb-84f3-4373-a87e-ac995db5a7b4.c000.snappy.parquet"
            order_qt_test_113 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-20183a04-7004-41dd-8c81-7c4e3be5a564.c000.snappy.parquet"
            order_qt_test_114 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-3333ba5b-7e65-4e34-8bc1-cfa6a83bd43a-c000.snappy.parquet"
            order_qt_test_115 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-c6d9240a-af02-4083-bcc6-e147b3acc8a1-c000.snappy.parquet"
            order_qt_test_116 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-e3033d59-d718-45ca-ac8b-a2fa769c51b8.c000.snappy.parquet"
            order_qt_test_117 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-7d256d6e-e8e3-4448-b206-71ed75f919b7-c000.snappy.parquet"
            order_qt_test_118 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/time-micros.parquet"
            test {
                sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """
                exception "The column type of 'member0' is not supported: INT64 => TimeV2"
            }


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00013-538d6279-0da7-4ef0-82e6-ae14dfbffc7c-c000.snappy.parquet"
            order_qt_test_120 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-d9fc223e-4feb-4848-9d25-03962fc0e540.c000.snappy.parquet"
            order_qt_test_121 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-a956ee76-bb52-49f2-8ddc-8bae79a3a44f-c000.snappy.parquet"
            order_qt_test_122 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-54afe94b-abc3-46fe-875a-99a89e12c4ec.c000.snappy.parquet"
            order_qt_test_123 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-0c74f71e-bb8b-4156-8560-bd819942f7fd.c000.snappy.parquet"
            order_qt_test_124 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-ad595774-679c-4e7b-a2f7-197d09fb86b0-c000.snappy.parquet"
            order_qt_test_125 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-74242cf5-7b73-4443-8b7a-fb5a7c23c5e2-c000.snappy.parquet"
            order_qt_test_126 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-0d4d7e36-7318-461a-8895-69a2e8e7df76-c000.snappy.parquet"
            order_qt_test_127 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-d9bbfa00-9941-4d2c-ac22-91d3e389f39a.c000.snappy.parquet"
            order_qt_test_128 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-3c5e6215-4af1-4536-b0d3-309af64e0d11.c000.snappy.parquet"
            order_qt_test_129 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/00000000000000000003.checkpoint.parquet"
            order_qt_test_130 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-194b12f4-b133-4363-81b6-aeaea00fff6d.c000.snappy.parquet"
            order_qt_test_131 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-a296d066-28da-44c1-96f0-18002fe4c0f9-c000.snappy.parquet"
            order_qt_test_132 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-6729fe1e-18b4-4f2e-b8a0-c23c38c540b1.c000.snappy.parquet"
            order_qt_test_133 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-dd98e67d-30f8-43ed-a4f8-667773604b4f-c000.snappy.parquet"
            order_qt_test_134 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00014-69bc5c81-628e-406d-b2b6-bcc70191f22c-c000.snappy.parquet"
            order_qt_test_135 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-eaf398e9-4e9e-4351-ba55-544e911c9b53.c000.snappy.parquet"
            order_qt_test_136 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-e22ef4cc-5ebf-41ef-a87f-6cb7d56de633-c000.snappy.parquet"
            order_qt_test_137 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-1cfba6d9-7cde-4d70-8b44-590dd2f3667d.c000.snappy.parquet"
            order_qt_test_138 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-cc7a4fbb-e7ef-4482-a911-1f37e5bfbafc.c000.snappy.parquet"
            order_qt_test_139 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-b3d21043-34ec-49e6-a606-4a453f2b2d5d-c000.snappy.parquet"
            order_qt_test_140 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-92431a70-4cc6-482b-8ae0-bdd568557439.c000.snappy.parquet"
            order_qt_test_141 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-2e28c8a6-b954-4681-9e80-deda01ec8115-c000.snappy.parquet"
            order_qt_test_142 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-84c719f6-e3c0-4f89-a3cc-ed07aaffc6c3-c000.snappy.parquet"
            order_qt_test_143 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-51d389f1-c888-4557-9dfd-1a3b47895897.c000.snappy.parquet"
            order_qt_test_144 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-5fdd8b77-5a63-4fbf-8192-7a3f270951fe.c000.snappy.parquet"
            order_qt_test_145 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-b1feb1d6-1c40-4672-93d0-4be4b3f07ce1.c000.snappy.parquet"
            order_qt_test_146 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-b87bb74a-b6ea-473e-9c7a-ccb0bc94bf19-c000.snappy.parquet"
            order_qt_test_147 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-a506a823-8408-4d69-a5b3-fd9a7157d46b-c000.snappy.parquet"
            order_qt_test_148 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-531c8750-ef01-49ca-92bf-f7af45a72ea0.c000.snappy.parquet"
            order_qt_test_149 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-6aed618a-2beb-4edd-8466-653e67a9b380.c000.snappy.parquet"
            order_qt_test_150 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-54033b0c-eae5-4470-acc8-c35d719b95a0-c000.snappy.parquet"
            order_qt_test_151 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-6f261ad3-ab3a-45e1-9047-01f9491f5a8c-c000.snappy.parquet"
            order_qt_test_152 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-aa99b42e-b5ca-4faf-a9ff-cbcd951cc1b7-c000.snappy.parquet"
            order_qt_test_153 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-72a56c23-01ba-483a-9062-dd0accc86599.c000.snappy.parquet"
            order_qt_test_154 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/00000000000000000020.checkpoint.parquet"
            order_qt_test_155 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-efdf1bb8-0e02-4d79-a16c-e402edd6b62a-c000.snappy.parquet"
            order_qt_test_156 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-3d546786-bedc-407f-b9f7-e97aa12cce0f.c000.snappy.parquet"
            order_qt_test_157 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-b945dfb5-9982-4f86-b903-dabef99caba1.c000.snappy.parquet"
            order_qt_test_158 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/00000000000000000001.checkpoint.0000000001.0000000001.90cf4e21-dbaa-41d6-8ae5-6709cfbfbfe0.parquet"
            order_qt_test_159 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-4869f9d0-efdb-4e6d-a7ab-1c7f2cb0d8fa.c000.snappy.parquet"
            order_qt_test_160 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-a2ff44be-f94c-4c1c-8fc0-342c273b22e1.c000.snappy.parquet"
            order_qt_test_161 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-40dd1707-1d42-4328-a59a-21f5c945fe60.c000.snappy.parquet"
            order_qt_test_162 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-a3b8b550-b0f5-4585-beef-fc82aea5d233-c000.snappy.parquet"
            order_qt_test_163 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-c915c6e2-8458-48ee-bf1f-575f9715f307.c000.snappy.parquet"
            order_qt_test_164 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-4a4be67a-1d35-4c93-ac3d-c1b26e2c5f3a-c000.snappy.parquet"
            order_qt_test_165 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-4e7c8482-82b7-49a7-882b-169167f7b2e4-c000.snappy.parquet"
            order_qt_test_166 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-d7e268e6-197c-4e9e-a8c4-03ad0c6ab228-c000.snappy.parquet"
            order_qt_test_167 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-11905402-f412-4aa0-9bb7-a033a6a5c2a4.c000.snappy.parquet"
            order_qt_test_168 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-b7dfce22-3171-499c-b0e8-ee979d5a40ef-c000.snappy.parquet"
            order_qt_test_169 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/4a3fcb9b-65eb-4f6e-acf9-7b0764bb4dd1-0_0-70-2444_20220906063456550.parquet"
            order_qt_test_170 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-9d31f3c5-f912-4828-a4eb-1410902f1f87-c000.snappy.parquet"
            order_qt_test_171 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-b39d205b-cb2f-4244-ac37-14f35cf9fd51-c000.snappy.parquet"
            order_qt_test_172 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-9ae22229-3ef0-40d3-8aaf-5e76fb8289c4.c000.snappy.parquet"
            order_qt_test_173 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-9ea0e2f3-b7f8-434d-8a3a-ba2be3be23a1-c000.snappy.parquet"
            order_qt_test_174 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-ae91ccaa-9203-4bb6-8e73-ed1653c83e11-c000.snappy.parquet"
            order_qt_test_175 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-aceaf062-1cd1-45cb-8f83-277ffebe995c.c000.snappy.parquet"
            order_qt_test_176 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-7f656c31-4815-4177-b16e-4f588eb187a9-c000.snappy.parquet"
            order_qt_test_177 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-a6d783a7-e1fc-4914-87c2-7e026180d74b.c000.snappy.parquet"
            order_qt_test_178 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-8f59e289-fc7e-4ed9-9eff-83850a495126-c000.snappy.parquet"
            order_qt_test_179 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-63ccacf3-142b-464b-9888-7e1554c64220-c000.snappy.parquet"
            order_qt_test_180 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00002-7fa3ac21-a7db-4aa9-912c-e1eda2660e1e.c000.snappy.parquet"
            order_qt_test_181 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-9dc5a505-e7ae-4958-901d-f05b8f6a5209-c000.snappy.parquet"
            order_qt_test_182 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/00000000000000000010.checkpoint.parquet"
            order_qt_test_183 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00010-155b9899-dcfa-458f-a6a4-fdde6401871b-c000.snappy.parquet"
            order_qt_test_184 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-b82d8859-84a0-4f05-872c-206b07dd54f0.c000.snappy.parquet"
            order_qt_test_185 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-8517b290-e7c8-488b-bcdb-029051af81ae.c000.snappy.parquet"
            order_qt_test_186 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-42b7bd7e-34ab-4981-b4ae-51f9b3421a9f-c000.snappy.parquet"
            order_qt_test_187 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-da80387a-f286-4848-9853-f82afd408710.c000.snappy.parquet"
            order_qt_test_188 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-98195ace-e492-4cd7-97d1-9b955202874b-c000.snappy.parquet"
            order_qt_test_189 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-0db54306-c35a-4d81-adbb-1e0600cda584.c000.snappy.parquet"
            order_qt_test_190 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-53b84700-3193-4a47-ad24-3928690b7643.c000.snappy.parquet"
            order_qt_test_191 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-75bd12c9-bafd-41dc-b1ae-5a8382308137.c000.snappy.parquet"
            order_qt_test_192 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/167a0e3e-9b94-444f-a178-242230cdb5a2-0_0-28-26_20211221030120532.parquet"
            order_qt_test_193 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-801608ec-3a0b-4cf0-a345-d861448a9146-c000.snappy.parquet"
            order_qt_test_194 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-a9430040-28e5-4007-8be2-843ac4eeb489-c000.snappy.parquet"
            order_qt_test_195 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-8dce356b-555b-4f79-9acb-1d0d8caa7194-c000.snappy.parquet"
            order_qt_test_196 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-74e18028-cf45-41fc-8cb6-b420018fdeaf.c000.snappy.parquet"
            order_qt_test_197 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-000b1742-6505-408b-a0f2-cbd87d6cd3db.c000.snappy.parquet"
            order_qt_test_198 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-584f8d6d-7e8e-4ba2-8b2c-02ec917b7179-c000.snappy.parquet"
            order_qt_test_199 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-77e34410-74c8-4e07-8769-345baa22e4b3-c000.snappy.parquet"
            order_qt_test_200 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/20230703_050340_00000_tides-1a95a95b-9dd7-4604-8d65-6d1448179007.parquet"
            order_qt_test_201 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-8f81c841-6afe-445e-bd40-5531f4ad6164.c000.snappy.parquet"
            order_qt_test_202 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-8d561a88-21d7-49de-9311-6b7af3a5f0cf.c000.snappy.parquet"
            order_qt_test_203 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/20230702_050337_00000_6kfs7-2ec0f5f9-d16d-4147-8810-e9e2ae44d5a4.parquet"
            order_qt_test_204 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-24bc0846-e2b9-4fd7-9a7f-e0b418125932.c000.snappy.parquet"
            order_qt_test_205 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-a27d2c20-2300-48ec-b6cb-902b29121ae2-c000.snappy.parquet"
            order_qt_test_206 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-aaaccec3-1d98-4fa6-8ad7-4b9100e46b14-c000.snappy.parquet"
            order_qt_test_207 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-621cad83-d386-48dc-99e4-f72bd8e2254b.c000.snappy.parquet"
            order_qt_test_208 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-d65a78f0-21bf-4d40-b9fe-1b89455d061e.c000.snappy.parquet"
            order_qt_test_209 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/00000000000000000006.checkpoint.0000000002.0000000002.parquet"
            order_qt_test_210 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-0fd35e0f-1dcc-4601-bbbd-0275e970c9a2-c000.snappy.parquet"
            order_qt_test_211 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-035e3c9f-e0a2-45df-9cbd-f72c7f466757-c000.snappy.parquet"
            order_qt_test_212 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-dfeb7019-307b-45a6-a716-86160c25ccf2.c000.snappy.parquet"
            order_qt_test_213 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00003-0f53cae3-3e34-4876-b651-e1db9584dbc3.c000.snappy.parquet"
            order_qt_test_214 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-c341dfbf-9a23-4046-9e35-087d8f5a5e80-c000.snappy.parquet"
            order_qt_test_215 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-c2ea82b3-cbf3-413a-ade1-409a1ce69d9c-c000.snappy.parquet"
            order_qt_test_216 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-2a7b0b03-9306-4b97-843e-10922f91b647.c000.snappy.parquet"
            order_qt_test_217 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-4403faf7-d608-4120-a423-64a7edb1743f.c000.snappy.parquet"
            order_qt_test_218 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00008-6e247bff-d0b1-4164-b5de-0f6bf7995dc3-c000.snappy.parquet"
            order_qt_test_219 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-8c3ec7f5-38a5-4ef3-ba1f-ad5734ac16af.c000.snappy.parquet"
            order_qt_test_220 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-19ad3a4c-f5fe-4547-a6ac-2d3d4abb3b9f.c000.snappy.parquet"
            order_qt_test_221 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/00000000000000000001.checkpoint.parquet"
            order_qt_test_222 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-f7553f3e-9622-4158-8e9d-8e5f06eb5a04-c000.snappy.parquet"
            order_qt_test_223 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-1028f6ea-23ab-4048-8ca6-ee602119a2b9.c000.snappy.parquet"
            order_qt_test_224 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-01abc26a-95b0-4179-8b49-d91a2410cd93-c000.snappy.parquet"
            order_qt_test_225 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-4b2022d9-27dc-42ce-a3be-1cebb7d303e4.c000.snappy.parquet"
            order_qt_test_226 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-22aeb477-f838-44b1-a897-4b0e7d009ae4.c000.snappy.parquet"
            order_qt_test_227 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-7b5e9086-abfc-4a58-8c2c-d775a1203dd3.c000.snappy.parquet"
            order_qt_test_228 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/00000000000000000002.checkpoint.parquet"
            order_qt_test_229 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00003-98b77405-274a-47bd-b19b-8f434edbc2e3-c000.snappy.parquet"
            order_qt_test_230 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-43372cd6-9b92-40cf-bac1-0224a06b4141-c000.snappy.parquet"
            order_qt_test_231 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-51bd41c3-6a8e-4e97-ae34-1ae6ade088d2.c000.snappy.parquet"
            order_qt_test_232 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-d44b300f-dae7-4fe3-957c-cc176edf237e.c000.snappy.parquet"
            order_qt_test_233 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-609e34b1-5466-4dbc-a780-2708166e7adb.c000.snappy.parquet"
            order_qt_test_234 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-85001c6e-1d42-49a1-b975-083e680642c4.c000.snappy.parquet"
            order_qt_test_235 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-721700d2-26d7-42a3-a8f9-b6601628ccd4.c000.snappy.parquet"
            order_qt_test_236 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-77ed21f7-bca0-4624-9ad5-70c86a3f84ab-c000.snappy.parquet"
            order_qt_test_237 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-b4942635-7f3f-45f2-8d4d-c11e34d4a399-c000.snappy.parquet"
            order_qt_test_238 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/int96_timestamps_nanos_outside_day_range.parquet"
            order_qt_test_239 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00016-304ef448-0163-4aa3-bfab-f3aaac90354e-c000.snappy.parquet"
            order_qt_test_240 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/00000000000000000006.checkpoint.0000000001.0000000002.parquet"
            order_qt_test_241 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-f1ee8b8b-3b35-4dfc-b502-e3605bda2996-c000.snappy.parquet"
            order_qt_test_242 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-cb64d459-76c9-486e-a259-9d160116bdd0-c000.snappy.parquet"
            order_qt_test_243 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-4137b6c1-34fd-4926-a939-dc6a01571d9f-c000.snappy.parquet"
            order_qt_test_244 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-05c5c5a6-fbe6-4912-b1b0-a4141d85203e-c000.snappy.parquet"
            order_qt_test_245 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-38ffb03f-2761-4a76-ae81-d0f39c47852f-c000.snappy.parquet"
            order_qt_test_246 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-7107c6ca-7e52-4c0a-8165-6b12e30869a2-c000.snappy.parquet"
            order_qt_test_247 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-785b7594-4202-4ccd-a8f3-d54fea18991b-c000.snappy.parquet"
            order_qt_test_248 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-cb908989-070d-4a9d-8ab4-bf0a07ee8ba2.c000.snappy.parquet"
            order_qt_test_249 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00004-53e7f54a-e774-40cd-9d49-d6f399913ec0-c000.snappy.parquet"
            order_qt_test_250 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-2eb23665-cadc-4fea-b573-83d27b6b19a4.c000.snappy.parquet"
            order_qt_test_251 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-1403b99b-7920-4f77-8095-86ce8096704d.c000.snappy.parquet"
            order_qt_test_252 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-b26c891a-7288-4d96-9d3b-bef648f12a34.c000.snappy.parquet"
            order_qt_test_253 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-4aecc0d9-dfbf-4180-91f1-4fd762dbc279.c000.snappy.parquet"
            order_qt_test_254 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-1c37312b-da71-4e99-9544-4bf52f42451c-c000.snappy.parquet"
            order_qt_test_255 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00003-0de75b5a-15f7-4798-9d7b-aa3ceb4432ff-c000.snappy.parquet"
            order_qt_test_256 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-dbbc308b-0878-4d8e-a814-d0b55ec894d4-c000.snappy.parquet"
            order_qt_test_257 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-90ba1b05-3e80-48dc-85fb-ef6b7085863a.c000.snappy.parquet"
            order_qt_test_258 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-df481541-fe59-4af2-a37f-68a39a1e2a5d-c000.snappy.parquet"
            order_qt_test_259 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-4298c516-6c96-4cc8-ae63-9228c71a6f2e-c000.snappy.parquet"
            order_qt_test_260 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-74e201ba-260c-4c9e-8252-23e9b8d522ac.c000.snappy.parquet"
            order_qt_test_261 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-b5fd762d-96da-42e4-ad38-ca62743fc7c4-c000.snappy.parquet"
            order_qt_test_262 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-d91910a6-311e-4bd2-a2e5-29340b02ef1d.c000.snappy.parquet"
            order_qt_test_263 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-4dca9c5e-1266-4706-932f-75419ab52cb5.c000.snappy.parquet"
            order_qt_test_264 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-7e43a3c3-ea26-4ae7-8eac-8f60cbb4df03.c000.snappy.parquet"
            order_qt_test_265 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00003-74fb7d5c-eda9-48c6-832a-13dd0178ad2e.c000.snappy.parquet"
            order_qt_test_266 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-b0b98900-4fe4-4c22-ad51-909b9600e221.c000.snappy.parquet"
            order_qt_test_267 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-dec1546d-b4ec-4f1c-9e32-d77f14fcd56d-c000.snappy.parquet"
            order_qt_test_268 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-07432bff-a65c-4b96-8775-074e4e99e771-c000.snappy.parquet"
            order_qt_test_269 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00015-75c18b70-9f74-4872-8a13-8a97aef1068a-c000.snappy.parquet"
            order_qt_test_270 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-1399f41f-eb04-4dde-ae03-d912a57094d7.c000.snappy.parquet"
            order_qt_test_271 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-03c15ed6-8778-435e-9ab9-467e83ec84dc-c000.snappy.parquet"
            order_qt_test_272 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-bd457a1b-4127-43e8-a7df-ef46a61f80f3.c000.snappy.parquet"
            order_qt_test_273 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-01da98d5-5186-49d7-bd68-477cd393b4d4-c000.zstd.parquet"
            order_qt_test_274 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/00000000000000000001.checkpoint.b6785ba8-b035-4760-b814-1f115ccb6167.parquet"
            order_qt_test_275 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-8f0c8ce9-cf36-47f2-8d5e-80d80c9d047b.c000.snappy.parquet"
            order_qt_test_276 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-ad851560-07d0-4bdf-a850-54c6e8211e4f-c000.snappy.parquet"
            order_qt_test_277 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            // uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/reordered_columns.parquet"
            // order_qt_test_278 """ select * from HDFS(
            //             "uri" = "${uri}",
            //             "hadoop.username" = "${hdfsUserName}",
            //             "format" = "parquet") limit 10; """
            // [vparquet_reader.cpp:753] Check failed: chunk_start >= last_chunk_end (54538 vs. 301886)


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-5488e09a-9998-4662-b5f4-e86e75cf68c6.c000.snappy.parquet"
            order_qt_test_279 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-81f5948d-6e60-4582-b758-2424da5aef0f-c000.snappy.parquet"
            order_qt_test_280 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-336ed04a-2371-4305-ab09-530769745601-c000.snappy.parquet"
            order_qt_test_281 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-0e22455f-5650-442f-a094-e1a8b7ed2271-c000.snappy.parquet"
            order_qt_test_282 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-bd00087c-6128-4c2e-a2c2-842d3628753e.c000.snappy.parquet"
            order_qt_test_283 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-b7fbbe31-c7f9-44ed-8757-5c47d10c3e81.c000.snappy.parquet"
            order_qt_test_284 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-cd45a0d6-4090-4c76-b3d9-b2b4a0712312.c000.snappy.parquet"
            order_qt_test_285 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-f61316e9-b279-4efa-94c8-5ababdacf768-c000.snappy.parquet"
            order_qt_test_286 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/time_millis_int32.snappy.parquet"
            test {
                sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """
                exception "The column type of 'column1' is not supported: INT32 => TimeV2"
            }


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-3794c463-cb0c-4beb-8d07-7cc1e3b5920f.c000.snappy.parquet"
            order_qt_test_288 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-4e939ac2-2487-4573-aa86-29a3b3e27d47-c000.snappy.parquet"
            order_qt_test_289 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-eea11d6e-8b50-408d-93a1-ec56027682aa.c000.snappy.parquet"
            order_qt_test_290 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-e494b9fe-464a-45ec-bdbd-96c71b5fa570-c000.snappy.parquet"
            order_qt_test_291 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-564b0b78-9796-45f9-8f3d-6f36fb01929b-c000.snappy.parquet"
            order_qt_test_292 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-099156d2-f7e7-4d31-bf57-14830d6d3659.c000.snappy.parquet"
            order_qt_test_293 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-eb5caa9d-1841-4bc4-9aee-5b09af4cb32b-c000.snappy.parquet"
            order_qt_test_294 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-7d5755b3-20b1-4039-920b-d1f24022685e.c000.snappy.parquet"
            order_qt_test_295 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-d1d2749d-e75a-4b52-ae5d-9d7f4b515f1d-c000.snappy.parquet"
            order_qt_test_296 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-0199254b-146e-48bb-afe8-a7e9be067d2c-c000.snappy.parquet"
            order_qt_test_297 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-e874d364-dd9e-472f-810d-90c9c985f6c8-c000.snappy.parquet"
            order_qt_test_298 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-9bfa0123-eb61-49a8-96f4-5d7a7615306b.c000.snappy.parquet"
            order_qt_test_299 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00007-ba2a46c8-01cc-4c86-abfa-0b9149a7e55a-c000.snappy.parquet"
            order_qt_test_300 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00012-ae4e5f8a-8577-4d73-b38e-ea7421a6f220-c000.snappy.parquet"
            order_qt_test_301 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-f46d1642-439e-44e7-a053-9ff458c21d09-c000.snappy.parquet"
            order_qt_test_302 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-f7483f4e-7e54-4f90-a754-eaa3481f2825.c000.snappy.parquet"
            order_qt_test_303 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-de494f2c-f5c2-4cb4-bad3-bb35e2e34b7c.c000.snappy.parquet"
            order_qt_test_304 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-c21fa19d-e3c0-4224-95c7-92311279966c-c000.snappy.parquet"
            order_qt_test_305 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00002-5800be2e-2373-47d8-8b86-776a8ea9d69f.c000.snappy.parquet"
            order_qt_test_306 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-b44eebb3-7b60-4667-b224-9863cd81ab6b.c000.snappy.parquet"
            order_qt_test_307 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-98a2ca07-6760-4bd4-91ed-a37dc39a7c2a.c000.snappy.parquet"
            order_qt_test_308 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-acb88926-db40-4001-a8f0-18348daa31ee.c000.snappy.parquet"
            order_qt_test_309 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-4493f4e8-b6b3-4fe8-a98b-29ac1d641b78.c000.snappy.parquet"
            order_qt_test_310 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-8d7c1b84-932d-4e1a-a23f-0a9e87a1ad68.c000.snappy.parquet"
            order_qt_test_311 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-c5c7f285-c008-4bc9-897e-5e6296ca92fa-c000.snappy.parquet"
            order_qt_test_312 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-3d5514b4-0b6f-47a5-83dd-5153056fe48f.c000.snappy.parquet"
            order_qt_test_313 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-afeef968-a917-4d51-a652-e5a4214df453.c000.snappy.parquet"
            order_qt_test_314 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-fc1ce4b4-742d-440b-af50-4bbac75a358c.c000.snappy.parquet"
            order_qt_test_315 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-1bef82fc-7e89-42f1-bf1d-aea11915464d-c000.snappy.parquet"
            order_qt_test_316 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-d0486dd4-1339-462b-9de1-d11cbe14e24f.c000.snappy.parquet"
            order_qt_test_317 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-da29c0d3-b5d6-425c-9f33-1296f2b97058.c000.snappy.parquet"
            order_qt_test_318 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-0b14a18c-7582-414b-969e-98a9075cf306.c000.snappy.parquet"
            order_qt_test_319 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-e92cd043-c41f-4e58-b739-2ea947542840-c000.snappy.parquet"
            order_qt_test_320 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-3aaa2967-ffcc-4b18-ad37-0178cc320e63.c000.snappy.parquet"
            order_qt_test_321 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-d6a0dd7d-8416-436d-bb00-245452904a81.c000.snappy.parquet"
            order_qt_test_322 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-c5e9359b-3522-4b41-ae08-af50a20b99a5-c000.snappy.parquet"
            order_qt_test_323 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-ab320428-f161-48de-a3ef-e4aff3b03447-c000.snappy.parquet"
            order_qt_test_324 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-17951bea-0d04-43c1-979c-ea1fac19b382-c000.snappy.parquet"
            order_qt_test_325 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-6951e6ec-f8d3-4d17-9154-621a959a63d1-c000.snappy.parquet"
            order_qt_test_326 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-f8d85c69-f8aa-4a9d-9d1e-c18f617fcd27-c000.snappy.parquet"
            order_qt_test_327 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-14cf0ed3-5e65-4d29-8fe3-362b279823e8-c000.snappy.parquet"
            order_qt_test_328 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-e1778fc8-5225-4b79-9eab-a5edc1b5b597-c000.snappy.parquet"
            order_qt_test_329 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-d84ba69f-2798-4a39-88d8-e324c1c8eb25.c000.snappy.parquet"
            order_qt_test_330 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-464939d6-b83c-4e84-b4ce-42d784af9615.c000.snappy.parquet"
            order_qt_test_331 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-652246ea-74e4-4ec4-9899-df13e5532393-c000.snappy.parquet"
            order_qt_test_332 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-808292c7-3cfb-4f68-9ae8-db6e899ef105-c000.snappy.parquet"
            order_qt_test_333 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00006-a7b871e3-1405-46ca-b151-c2df4821642e-c000.snappy.parquet"
            order_qt_test_334 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00003-177aeaf4-bafd-4cff-abaf-daadd575a598.c000.snappy.parquet"
            order_qt_test_335 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-3088f542-020d-4074-b24c-16634f0b1c62-c000.snappy.parquet"
            order_qt_test_336 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00019-436b8805-ae66-4eb4-93b1-2fc1f56fc7b5-c000.snappy.parquet"
            order_qt_test_337 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-245cae09-959e-45cd-99f4-0633484076f3.c000.snappy.parquet"
            order_qt_test_338 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-018377e7-48e6-451b-9982-e404e3969a7c.c000.snappy.parquet"
            order_qt_test_339 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-a4218de6-b786-43c6-b1dc-1d5bd449f4f8-c000.snappy.parquet"
            order_qt_test_340 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-294b55f0-6916-4600-9086-17516ec11e56.c000.snappy.parquet"
            order_qt_test_341 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-37ccfcd3-b44b-4d04-a1e6-d2837da75f7a.c000.snappy.parquet"
            order_qt_test_342 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-2fd18c09-d08a-4efe-990d-d84b2efce225.c000.snappy.parquet"
            order_qt_test_343 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00003-aa429af4-35df-4fff-81b7-2cb7220698f7-c000.snappy.parquet"
            order_qt_test_344 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/00000000000000000001.checkpoint.0000000001.0000000001.25992ab9-b145-4179-b0ee-2ecef614db23.parquet"
            order_qt_test_345 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00001-dd006b0f-93c6-4f87-801f-a8b7153a72ae-c000.snappy.parquet"
            order_qt_test_346 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/05b0f4ec-00fb-49f2-a1e2-7f510f3da93b-0_0-27-28_20231127051653361.parquet"
            order_qt_test_347 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-a133110e-9b29-4f68-b1ae-3a874d73c5dd.c000.snappy.parquet"
            order_qt_test_348 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-ebd78c56-1fa0-4d34-8a10-2a0c3300eed7.c000.snappy.parquet"
            order_qt_test_349 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/719c3273-2805-4124-b1ac-e980dada85bf-0_0-27-1215_20220906063435640.parquet"
            order_qt_test_350 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-590cf436-ab76-4290-b93b-a24810f54390-c000.snappy.parquet"
            order_qt_test_351 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-70cbd30d-efb0-4090-a7b4-c1de8083743b-c000.snappy.parquet"
            order_qt_test_352 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-89dcee5a-b180-433c-aa1c-1973ae8e1920.c000.snappy.parquet"
            order_qt_test_353 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-8e3f6979-c368-4f48-b389-9b4dbf46de93-c000.snappy.parquet"
            order_qt_test_354 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00000-30101bc7-16af-4e32-b306-84e03d69b3f8.c000.snappy.parquet"
            order_qt_test_355 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group4/part-00017-dd27ce7e-fcde-436b-99c3-a32e53f5b61b-c000.snappy.parquet"
            order_qt_test_356 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """
        } finally {
        }
    }
}
