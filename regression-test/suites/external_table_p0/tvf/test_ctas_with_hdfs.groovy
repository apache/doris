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

suite("test_ctas_with_hdfs","external,hive,tvf,external_docker") {
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    // It's okay to use random `hdfsUser`, but can not be empty.
    def hdfsUserName = "doris"
    def format = "orc"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String tableName = "ctas_tvf_1";
        def uri = "${defaultFS}" + "/user/doris/preinstalled_data/orc/orc_all_types/p1_col=desktops/p2_col=bigint_col/*"

        // test varchar type as partition column
        sql """drop table if exists ${tableName}; """
        sql """ create table ${tableName}
            PARTITION BY LIST(p1_col,p2_col) (
                PARTITION p1 VALUES IN (("desktops", "bigint_col")),
                PARTITION p2 VALUES IN (("phones", "float_col"))
            )
            PROPERTIES("replication_num" = "1") 
            as
            select * from
            HDFS(
                "uri" = "${uri}",
                "hadoop.username" = "${hdfsUserName}",
                "path_partition_keys"="p1_col,p2_col",
                "format" = "${format}") where tinyint_col < 4;
        """
        order_qt_desc """desc ${tableName};"""
        order_qt_select """ select * from ${tableName} order by tinyint_col"""

        // test char type as partition column
        tableName = "ctas_tvf_2";
        sql """drop table if exists ${tableName}; """
        sql """
            create table ${tableName}
            PARTITION BY LIST(char_col) (
                PARTITION p1 VALUES IN (("desktops"))
            )
            PROPERTIES("replication_num" = "1")
            as
            select * from
            HDFS(
                "uri" = "${uri}",
                "hadoop.username" = "${hdfsUserName}",
                "format" = "${format}") where tinyint_col < 4;
            """
        order_qt_desc_2 """desc ${tableName};"""
        order_qt_select_2 """ select * from ${tableName} order by tinyint_col"""

        // test tinyint_col type as partition column
        tableName = "ctas_tvf_3";
        sql """drop table if exists ${tableName}; """
        sql """
            create table ${tableName}
            PARTITION BY LIST(tinyint_col) (
                PARTITION p1 VALUES IN ((1)),
                PARTITION p2 VALUES IN ((2)),
                PARTITION p3 VALUES IN ((3))
            )
            PROPERTIES("replication_num" = "1")
            as
            select * from
            HDFS(
                "uri" = "${uri}",
                "hadoop.username" = "${hdfsUserName}",
                "format" = "${format}") where tinyint_col < 4;
            """
        order_qt_desc_3 """desc ${tableName};"""
        order_qt_select_3 """ select * from ${tableName} order by tinyint_col"""


        // test String as partition column
        tableName = "ctas_tvf_4";
        sql """drop table if exists ${tableName}; """
        sql """
            create table ${tableName}
            PARTITION BY LIST(string_col) (
                PARTITION p1 VALUES IN (("Width activation annoying. Speaker senator cultivate convention silence price second1. Ok personal formulate princess. Screening debt salary reluctant have circulate exclusion. Shy immigrant trousers fifteen cat opponent.")),
                PARTITION p2 VALUES IN (("Merge fulfil authentic. Paint honest keyboard wave live2. Involvement fighting suppose freeze investigate hers glass. Marriage celebrity shut philosophical."))
            )
            PROPERTIES("replication_num" = "1")
            as
            select * from
            HDFS(
                "uri" = "${uri}",
                "hadoop.username" = "${hdfsUserName}",
                "format" = "${format}") where smallint_col <11;
            """
        order_qt_desc_4 """desc ${tableName};"""
        order_qt_select_4 """ select * from ${tableName} order by tinyint_col"""
        
        // test varchar type as distributed column
        tableName = "ctas_tvf_5";
        sql """drop table if exists ${tableName}; """
        sql """ create table ${tableName}
            DISTRIBUTED BY HASH(p1_col) BUCKETS 10
            PROPERTIES("replication_num" = "1") 
            as
            select * from
            HDFS(
                "uri" = "${uri}",
                "hadoop.username" = "${hdfsUserName}",
                "path_partition_keys"="p1_col,p2_col",
                "format" = "${format}") where tinyint_col < 4;
        """
        order_qt_desc5 """desc ${tableName};"""
        order_qt_select5 """ select * from ${tableName} order by tinyint_col"""
    }
}
