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

suite("test_hdfs_parquet_group3","external,hive,tvf,external_docker") {
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def hdfsUserName = "doris"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def uri = ""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-f888e95b-c831-43fe-bba8-3dbf43b4eb86.c000.snappy.parquet"
            order_qt_test_0 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-09e47b80-36c2-4475-a810-fbd8e7994971-c000.snappy.parquet"
            order_qt_test_1 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-0441e99a-c421-400e-83a1-212aa6c84c73-c000.snappy.parquet"
            order_qt_test_2 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-782da9d6-35c0-400c-a79e-acc620f10122.c000.snappy.parquet"
            order_qt_test_3 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-f40d1bcb-0cf4-4d02-8ff8-5dcf0511833c.c000.snappy.parquet"
            order_qt_test_4 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00004-2070fd43-0a25-4b00-920c-4fddcaefd63b.c000.snappy.parquet"
            order_qt_test_5 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00003-d9b49db4-8ee1-443c-b30c-556080517199.c000.snappy.parquet"
            order_qt_test_6 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-b8249b87-0b7a-4461-8a8a-fa958802b523-c000.snappy.parquet"
            order_qt_test_7 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00006-bd18dcba-5d95-4555-88f1-1f8da33c04b3.c000.snappy.parquet"
            order_qt_test_8 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-0a72544a-fb83-4eaa-8d62-9e6ab59afa8b.c000.snappy.parquet"
            order_qt_test_9 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-bfef93f3-f896-4a83-87bd-619b80c60a38.c000.snappy.parquet"
            order_qt_test_10 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-34c8c673-3f44-4fa7-b94e-07357ec28a7d-c000.snappy.parquet"
            order_qt_test_11 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00005-878d1d0c-6f7b-4a16-b8e1-2acba456f9ac.c000.snappy.parquet"
            order_qt_test_12 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00005-0913f889-3d12-448d-84f0-28ae28a01df3-c000.snappy.parquet"
            order_qt_test_13 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-b13d0db9-2ad3-41ae-bceb-0b3951fed23e-c000.snappy.parquet"
            order_qt_test_14 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00005-1f25d9f3-3e57-4fd2-aa46-071750852762.c000.snappy.parquet"
            order_qt_test_15 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/data.parquet"
            order_qt_test_16 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-dcba864f-ad9d-4eaf-87a4-f1007bb0aff0.c000.snappy.parquet"
            order_qt_test_17 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-0f755735-3b5b-449a-8f93-92a40d9f065d-c000.snappy.parquet"
            order_qt_test_18 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00003-c308586a-40e9-4dcd-9837-f08cb65f5eed.c000.snappy.parquet"
            order_qt_test_19 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00004-4d39fa0a-62f7-4045-b2c4-1e860e6379e7.c000.snappy.parquet"
            order_qt_test_20 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00005-59f727dd-7597-47ef-96ff-c83eebe882e5.c000.snappy.parquet"
            order_qt_test_21 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-999ae312-021c-4823-b0b6-08014cea263a-c000.snappy.parquet"
            order_qt_test_22 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00003-84dfa6d6-7e0a-41e8-a12b-077ac23cf60c.c000.snappy.parquet"
            order_qt_test_23 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-a0b7e955-f3e8-4f06-932d-b0b40bc05252.c000.snappy.parquet"
            order_qt_test_24 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-2dba4ccf-bbcd-4088-a34a-a0ed6ed00f00.c000.snappy.parquet"
            order_qt_test_25 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00005-02afd234-0c50-40aa-860e-10ddc7d2e35b.c000.snappy.parquet"
            order_qt_test_26 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-7bce012e-f358-4a97-91da-55c4d3266fbe.c000.snappy.parquet"
            order_qt_test_27 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-ebbb0a3a-0bd8-43ca-945e-a0ad938e56f4.c000.snappy.parquet"
            order_qt_test_28 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-62cff6a6-759f-4a56-9175-bb0e2d15dba5-c000.snappy.parquet"
            order_qt_test_29 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-f7dbf1cd-7f74-40c2-b097-832286326b3e.c000.snappy.parquet"
            order_qt_test_30 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-1d73763d-9fb7-4501-afc7-3aa8cda3dcd0-c000.snappy.parquet"
            order_qt_test_31 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-ef621d70-dcb7-40b8-b391-c915e54578e0.c000.snappy.parquet"
            order_qt_test_32 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-19206437-7615-4800-bd56-f9d3c596918e-c000.snappy.parquet"
            order_qt_test_33 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00005-7f1cdbf0-4ad2-42ce-b941-0ab2775278dc.c000.snappy.parquet"
            order_qt_test_34 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-d3dc64e6-0bf0-409b-a56d-7d24360e8f89-c000.snappy.parquet"
            order_qt_test_35 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-0af47301-edc9-4bda-ade7-c0ab17933654.c000.snappy.parquet"
            order_qt_test_36 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-4aa85334-6e99-4dac-9d75-dfd59ec37250-c000.snappy.parquet"
            order_qt_test_37 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-e22924ef-f192-483a-9b57-82d2123ba08e-c000.snappy.parquet"
            order_qt_test_38 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00005-f46ce121-5930-4cb7-855a-1f7499b215f9-c000.snappy.parquet"
            order_qt_test_39 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-e62ca5a1-923c-4ee6-998b-c61d1cfb0b1c-c000.snappy.parquet"
            order_qt_test_40 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-e20bae81-3f27-4c5c-aeca-5cfa6b38615c.c000.snappy.parquet"
            order_qt_test_41 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-b2e29782-7e1e-4819-a241-b5a9b5e910e8.c000.snappy.parquet"
            order_qt_test_42 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-a90a1e4f-c526-4f44-ad35-4a25f4fd2be7-c000.snappy.parquet"
            order_qt_test_43 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00003-00746a95-8d85-4916-a594-4bde8f6fda5d.c000.snappy.parquet"
            order_qt_test_44 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/b3711ddf-8c11-4666-82ec-fbc952e1dc72-0_1-61-24052_20210524095413.parquet"
            order_qt_test_45 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00006-b98aa941-fb21-482a-a9ed-b78bb7d435b0.c000.snappy.parquet"
            order_qt_test_46 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-27b57b04-e80f-48b2-9064-c459e9d26a87-c000.snappy.parquet"
            order_qt_test_47 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-bf15b1cf-2d02-47fd-aa12-1117de3f9071-c000.snappy.parquet"
            order_qt_test_48 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-e877c5f2-5fb9-4528-a317-5b30608bb45c-c000.snappy.parquet"
            order_qt_test_49 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-15854bd4-f450-4a20-8a8a-7d914751b230-c000.snappy.parquet"
            order_qt_test_50 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-c87aeb63-6d9c-4511-b8b3-71d02178554f.c000.snappy.parquet"
            order_qt_test_51 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-6ac325b3-2e9c-4a17-8f8c-bc2704387397-c000.snappy.parquet"
            order_qt_test_52 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00007-1c937176-1eec-48f4-b683-1e0b0df301da-c000.snappy.parquet"
            order_qt_test_53 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-01e41ef4-c5f0-4c25-a70a-0bf797e4021e.c000.snappy.parquet"
            order_qt_test_54 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-67f9fa1a-1a50-4ab4-b05c-9f27e4282f9e-c000.snappy.parquet"
            order_qt_test_55 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00003-3e7e1614-add1-41ea-91a7-c0205515cc11.c000.snappy.parquet"
            order_qt_test_56 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00005-2d4e572d-5bdd-43f4-9d13-7c5354deb1f6.c000.snappy.parquet"
            order_qt_test_57 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00004-9042e69b-b6b1-4c2b-977e-c1ad18e0e881.c000.snappy.parquet"
            order_qt_test_58 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00007-0f168d4b-1c64-4d11-8c80-aee4bf0ae8ba-c000.snappy.parquet"
            order_qt_test_59 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-842017c2-3e02-44b5-a3d6-5b9ae1745045-c000.snappy.parquet"
            order_qt_test_60 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/hive_generated.lz4.parquet"
            order_qt_test_61 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00006-d7a37fad-eaf7-43a7-8f25-6a2b63155bf5.c000.snappy.parquet"
            order_qt_test_62 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-13921577-6635-457e-9e3a-f32dc7fea982.c000.snappy.parquet"
            order_qt_test_63 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-b31c437d-93a7-462c-b88b-f8b49370d785.c000.snappy.parquet"
            order_qt_test_64 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/00000000000000000010.checkpoint.parquet"
            order_qt_test_65 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00004-05892139-fe13-4344-b069-ac213044a04c.c000.snappy.parquet"
            order_qt_test_66 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00005-5e5fd8c6-30ba-4d3a-a0c5-3aa520101b6e.c000.snappy.parquet"
            order_qt_test_67 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-b9c6b926-a274-4d8e-b882-31c4aac05038.c000.snappy.parquet"
            order_qt_test_68 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-df985307-35c6-4c43-9635-64f6e9ce9e3b-c000.snappy.parquet"
            order_qt_test_69 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00005-6521d2ed-6361-4bbf-9ad0-688ea20f5e70-c000.snappy.parquet"
            order_qt_test_70 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-8a40c3d2-f658-4131-a17f-388265ab04b7.c000.snappy.parquet"
            order_qt_test_71 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00003-93ecd48d-f465-49c4-8abf-83d8e4b0b33b-c000.snappy.parquet"
            order_qt_test_72 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-a65a7a56-3eb1-4886-bd24-1cbf76b9167e.c000.snappy.parquet"
            order_qt_test_73 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00003-6989c40a-2ab2-4ac3-815a-c43b7c4d491a.c000.snappy.parquet"
            order_qt_test_74 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00007-00fd7022-5e9c-4cba-b8c8-2297ef36e5ff.c000.snappy.parquet"
            order_qt_test_75 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-2a830e69-78f3-4d09-9b2c-3bfd9debc2f0.c000.snappy.parquet"
            order_qt_test_76 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-e1f83a41-9548-4c88-a6c7-6bad89420776.c000.snappy.parquet"
            order_qt_test_77 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-4c732f0f-a473-400a-8ba3-1499f599b8f1.c000.snappy.parquet"
            order_qt_test_78 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00006-7bb84938-7f7d-4f86-83be-3fae8bbdc52a-c000.snappy.parquet"
            order_qt_test_79 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-4fb7f274-cd1b-484d-95d3-42521e10e10d-c000.snappy.parquet"
            order_qt_test_80 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-3dcad520-b001-4829-a6e5-3d578b0964f4.c000.snappy.parquet"
            order_qt_test_81 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00005-397fde20-dffd-4ab1-8a1c-cb6dd73e50ee.c000.snappy.parquet"
            order_qt_test_82 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-d9004e55-077b-4728-9ee6-b3401faa46ba-c000.snappy.parquet"
            order_qt_test_83 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-c6ca97f6-1ef6-432f-a4ec-a4675be3c7af.c000.snappy.parquet"
            order_qt_test_84 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00006-5b9a2cd1-bacd-44f7-a017-b279c8ad7d27.c000.snappy.parquet"
            order_qt_test_85 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/spark_generated.lz4.parquet"
            order_qt_test_86 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-5d1ee207-355b-47ee-b58d-5cf32a3b8349-c000.snappy.parquet"
            order_qt_test_87 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-197ae400-931d-4eb2-8059-a4688c2d59a9-c000.snappy.parquet"
            order_qt_test_88 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-6c239ff5-7f3e-4bbd-b06a-4ea89364c08a.c000.snappy.parquet"
            order_qt_test_89 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-eaf1edf4-b9da-4df8-b957-08583e2a1d1b.c000.snappy.parquet"
            order_qt_test_90 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-a12a3c66-8abb-40a8-8d69-bbf7e516b710-c000.snappy.parquet"
            order_qt_test_91 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-ba244fcb-eded-4f94-9268-dfbde3cc20c1.c000.snappy.parquet"
            order_qt_test_92 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-82368d1d-588b-487a-be01-16dc85260296.c000.snappy.parquet"
            order_qt_test_93 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-3222be15-ab04-414e-a847-6b80c66de177-c000.snappy.parquet"
            order_qt_test_94 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00004-18dc7f87-95ac-415a-8be1-e9026bdc1309.c000.snappy.parquet"
            order_qt_test_95 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-4f02a740-31dc-46c6-bc0e-c19d164ac82d.c000.snappy.parquet"
            order_qt_test_96 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-67b6882e-f49f-4df5-9850-b5e8a72f4917.c000.snappy.parquet"
            order_qt_test_97 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-2e274fe7-eb75-4b73-8c72-423ee747abc0-c000.snappy.parquet"
            order_qt_test_98 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-106da899-cbd5-491f-b560-60792edbe807-c000.snappy.parquet"
            order_qt_test_99 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-7f8faf7f-2d0f-42c8-84c9-85df909dba39-c000.snappy.parquet"
            order_qt_test_100 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-2e12af63-21db-42d7-9bda-766b1255e6f6-c000.snappy.parquet"
            order_qt_test_101 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00006-07651b7b-32e7-4638-bf6b-1099e5fe7dae.c000.snappy.parquet"
            order_qt_test_102 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-9c2ae306-eeca-4e27-af5c-88e4536e5bd3-c000.snappy.parquet"
            order_qt_test_103 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-4f2f0b9f-50b3-4e7b-96a1-e2bb0f246b06-c000.snappy.parquet"
            order_qt_test_104 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00003-5e5271ca-618c-4572-af97-5954b4ce162a.c000.snappy.parquet"
            order_qt_test_105 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-ea50011b-7810-4e10-ab00-aa8463afc93c.c000.snappy.parquet"
            order_qt_test_106 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-a90c681e-2310-4bbc-8b56-e81b14aa1817-c000.snappy.parquet"
            order_qt_test_107 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00007-dfc8bc83-04f4-4f5f-8bd4-e9be33a8510a-c000.snappy.parquet"
            order_qt_test_108 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-b18c779a-23dd-4e66-b070-1ff779001b75-c000.snappy.parquet"
            order_qt_test_109 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00004-4550f7f1-1469-4caa-bc69-13da36d984aa.c000.snappy.parquet"
            order_qt_test_110 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/d0875d00-483d-4e8b-bbbe-c520366c47a0-0_0-6-11_20211217110514527.parquet"
            order_qt_test_111 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-3d30d085-4cde-471e-a396-12af34a70812-c000.snappy.parquet"
            order_qt_test_112 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-fe79b4e1-aaca-4bef-9a72-00c6def5b90c.c000.snappy.parquet"
            order_qt_test_113 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-09b4f0d0-4932-4d00-9a26-7f5711abefb7-c000.snappy.parquet"
            order_qt_test_114 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-4fa09d34-fc12-47ac-b571-8e3cec65b42d.c000.snappy.parquet"
            order_qt_test_115 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00007-ceb107e1-d878-4ecf-95f1-ae1767bcbd5d-c000.snappy.parquet"
            order_qt_test_116 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-8e5e2719-f9d0-4e23-8c27-1ac72563c6ab.c000.snappy.parquet"
            order_qt_test_117 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-cb078bc1-0aeb-46ed-9cf8-74a843b32c8c-c000.snappy.parquet"
            order_qt_test_118 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-de1d5bcd-ad7e-4b88-ba9b-31fb8aeb8093.c000.snappy.parquet"
            order_qt_test_119 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00006-3a9a8636-965a-43eb-9cbd-9f0afde18ed2.c000.snappy.parquet"
            order_qt_test_120 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-889df1d9-6d79-4ea3-8b69-971cec9bf618.c000.snappy.parquet"
            order_qt_test_121 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-84127988-1dfb-4051-929f-4cd9d87572c0.c000.snappy.parquet"
            order_qt_test_122 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00006-51231cb8-4cd3-4b1c-997c-de6fcf940f22.c000.snappy.parquet"
            order_qt_test_123 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/addressbook.parquet"
            order_qt_test_124 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-ec6e3a2e-ecbf-4d39-9076-37e523cd62f1.c000.snappy.parquet"
            order_qt_test_125 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00004-4f115879-4c36-4574-addd-463782349ac4-c000.snappy.parquet"
            order_qt_test_126 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-8354f522-7f84-4b2f-9bc0-ae7baea3f719.c000.snappy.parquet"
            order_qt_test_127 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-3a090e55-ad26-4e2e-8991-5fc96e11200a-c000.snappy.parquet"
            order_qt_test_128 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-3d845a07-363d-4712-9494-8a44353ebf16.c000.snappy.parquet"
            order_qt_test_129 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00003-6dbf3cc5-0294-49d5-a40f-79c05a093c66.c000.snappy.parquet"
            order_qt_test_130 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-52178ba6-0f7b-42f3-8c16-35b001e487d7.c000.snappy.parquet"
            order_qt_test_131 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00004-667b57bd-4960-450d-b445-63ec7dbd0c5e.c000.snappy.parquet"
            order_qt_test_132 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-ce66c2ca-8fdf-48d3-a6e7-5980a370461a.c000.snappy.parquet"
            order_qt_test_133 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-9bf4b8f8-1b95-411b-bf10-28dc03aa9d2f-c000.snappy.parquet"
            order_qt_test_134 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-f2547b28-9219-4628-8462-cc9c56edfebb-c000.snappy.parquet"
            order_qt_test_135 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-ae3312c4-7513-410d-985d-f2cf8da57313.c000.snappy.parquet"
            order_qt_test_136 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-967c6c2d-d830-49f6-b8fe-aeb018f3e719-c000.snappy.parquet"
            order_qt_test_137 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00004-c22f1d98-7785-4b6c-94d4-a022d6015f13.c000.snappy.parquet"
            order_qt_test_138 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-182665f0-30df-470d-a5cb-8d9d483ed390-c000.snappy.parquet"
            order_qt_test_139 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-17cf010c-a78b-4e61-a192-b240bc89f4fe-c000.snappy.parquet"
            order_qt_test_140 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00003-a5d95875-eb52-433c-96fd-28fc3b6c38eb-c000.snappy.parquet"
            order_qt_test_141 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00005-a8cca1e4-a3de-4c6e-9882-22a7383e7a8a-c000.snappy.parquet"
            order_qt_test_142 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00002-541f0ff8-f399-4095-b63d-289e15c401ea.c000.snappy.parquet"
            order_qt_test_143 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-5705917d-d837-4d7f-b8c4-f0ada8cf9663.c000.snappy.parquet"
            order_qt_test_144 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00005-d610330e-db0e-42d9-9636-266d237e4724.c000.snappy.parquet"
            order_qt_test_145 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-12ef2b67-494f-4eff-8e56-2431d0adf4bb-c000.snappy.parquet"
            order_qt_test_146 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00003-793e9573-2603-4a3d-ad36-24f5e1199c70.c000.snappy.parquet"
            order_qt_test_147 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-e3388985-08fd-4f28-81d7-c922eb7c0cac-c000.snappy.parquet"
            order_qt_test_148 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-4a9efe8d-12b3-4134-a932-a4e288cadf35-c000.snappy.parquet"
            order_qt_test_149 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00006-d01a6271-4c3a-4b0c-b723-eb91a4e8bc5a.c000.snappy.parquet"
            order_qt_test_150 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00001-529ff89b-55c6-4405-a6cc-04759d5f692b.c000.snappy.parquet"
            order_qt_test_151 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00004-ebbbe213-7720-4d7c-91a4-8c7444573266.c000.snappy.parquet"
            order_qt_test_152 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-64680d94-9e18-4fa1-9ca9-f0cd8a9cfd11-c000.snappy.parquet"
            order_qt_test_153 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00005-8f1cbfda-c8a6-4318-b6b6-d9e98a93a41f-c000.snappy.parquet"
            order_qt_test_154 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-202423c0-10bd-4dab-bff4-ac3e2338bfd8-c000.snappy.parquet"
            order_qt_test_155 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """


            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_parquet/group3/part-00000-e048b59b-38c6-417a-b452-41539eadc475-c000.snappy.parquet"
            order_qt_test_156 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "parquet") limit 10; """
        } finally {
        }
    }
}
