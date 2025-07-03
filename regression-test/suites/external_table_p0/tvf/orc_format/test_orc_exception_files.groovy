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

suite("test_orc_exception_files","external,hive,tvf,external_docker") {
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def hdfsUserName = "doris"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def uri = ""

    String enabled = context.config.otherConfigs.get("enableHiveTest")

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        test {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group5/corrupted.orc"
            sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """
            exception "Footer is corrupt: STRUCT type 0 has 3 subTypes, but has 2 fieldNames"
        }

        test {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group3/DwrfStripeCache_BOTH_AllStripes.orc"
            sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """
            exception "Init OrcReader failed. reason = Failed to parse the footer"
        }

        test {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group3/DwrfStripeCache_FOOTER_AllStripes.orc"
            sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """
            exception "Init OrcReader failed. reason = Failed to parse the footer from"
        }

        test {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group3/DwrfStripeCache_FOOTER_HalfStripes.orc"
            sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """
            exception "Init OrcReader failed. reason = Failed to parse the footer from"
        }


        test {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group3/DwrfStripeCache_BOTH_HalfStripes.orc"
            sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """
            exception "Init OrcReader failed. reason = Failed to parse the footer from"
        }


        test {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group3/DwrfStripeCache_INDEX_AllStripes.orc"
            sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """
            exception "Init OrcReader failed. reason = Failed to parse the footer from"
        }

        test {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group3/DwrfStripeCache_INDEX_HalfStripes.orc"
            sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """
            exception "Init OrcReader failed. reason = Failed to parse the footer from"
        }

        test {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group3/DwrfStripeCache_NONE.orc"
            sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """
            exception "Init OrcReader failed. reason = Failed to parse the footer from"
        }


        test {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group2/before_1582_ts_v2_4.snappy.orc"
            sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """
            exception "Orc row reader nextBatch failed. reason = Can't open /usr/share/zoneinfo/PST"
        }

        test {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/missing_blob_stream_in_string_dict.orc"
            sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """
            exception "Orc row reader nextBatch failed. reason = DICTIONARY_DATA stream not found in StringDictionaryColumn"
        }


        test {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/missing_length_stream_in_string_dict.orc"
            sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """
            exception "Orc row reader nextBatch failed. reason = LENGTH stream not found in StringDictionaryColumn"
        }

        test {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/negative_dict_entry_lengths.orc"
            sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """
            exception "Orc row reader nextBatch failed. reason = Negative dictionary entry length"
        }

        test {
            uri = "${defaultFS}" + "/user/doris/tvf_data/test_hdfs_orc/group0/stripe_footer_bad_column_encodings.orc"
            sql """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "orc"); """
            exception "Orc row reader nextBatch failed. reason = bad StripeFooter from zlib"
        }
    }
}