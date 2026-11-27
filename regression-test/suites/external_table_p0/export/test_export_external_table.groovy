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

import org.apache.doris.regression.util.ExportTestHelper

suite("test_export_external_table", "p0,external") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    def waiting_export = { ctlName, dbName, export_label ->
        while (true) {
            def res = sql """ show export from ${ctlName}.${dbName} where label = "${export_label}" """
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                break;
            } else if (res[0][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[0][10]}""")
            } else {
                sleep(5000)
            }
        }
    }

    // this table name must be `test1`, because this is an external table.
    def table_export_name = "test1"
    def outfile_path_prefix = "regression/test_export_external_table"

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def helper = new ExportTestHelper(delegate)
        String catalog_name = "test_export_external_table";
        String internal_db_name = "regression_test_external_table_p0_export";
        String ex_db_name = "doris_test";
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");

        sql """create database if not exists ${internal_db_name}; """

        sql """drop catalog if exists ${catalog_name} """
        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false&zeroDateTimeBehavior=convertToNull",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        sql  """ use ${internal_db_name} """

        qt_sql """select current_catalog()"""
        sql """switch ${catalog_name}"""
        qt_sql """select current_catalog()"""
        sql """ use ${ex_db_name}"""

        order_qt_export_table """ select * from ${table_export_name} where k8 < 100 order by k8; """

        // 1. basic test
        def uuid = UUID.randomUUID().toString()
        def outFilePath = "${outfile_path_prefix}/${table_export_name}_${uuid}"
        def label = "label_${uuid}"
        try {
            // exec export
            sql """
                EXPORT TABLE ${table_export_name} where k8 < 100
                TO "${helper.uri(outFilePath)}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "csv",
                    "column_separator"=","
                ) WITH S3 (${helper.storageProperties()});
            """
            waiting_export.call(catalog_name, ex_db_name, label)

            def export_res = sql """show export where label='${label}'"""
            logger.info("get export res ${export_res}")
            def outfileinfo = export_res[0][11];
            logger.info("get export outfile info ${outfileinfo}")
        } finally {
            helper.deleteFiles("${outFilePath}/")
        }

        // 2. export external table under internal catalog
        uuid = UUID.randomUUID().toString()
        outFilePath = "${outfile_path_prefix}/${table_export_name}_${uuid}"
        label = "label_${uuid}"
        try {
            sql """ switch internal """
            // exec export
            sql """
                EXPORT TABLE ${catalog_name}.${ex_db_name}.${table_export_name} where k8 < 100
                TO "${helper.uri(outFilePath)}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "csv",
                    "column_separator"=","
                ) WITH S3 (${helper.storageProperties()});
            """

            waiting_export.call(catalog_name, ex_db_name, label)

            def export_res = sql """show export from ${catalog_name}.${ex_db_name} where label='${label}'"""
            logger.info("get external export res ${export_res}")
            def outfileinfo = export_res[0][11];
            logger.info("get external export outfile info ${outfileinfo}")

        } finally {
            helper.deleteFiles("${outFilePath}/")
        }

        sql """ switch ${catalog_name} """
        // 3. csv_with_names
        uuid = UUID.randomUUID().toString()
        outFilePath = "${outfile_path_prefix}/${table_export_name}_${uuid}"
        label = "label_${uuid}"
        try {
            // exec export
            sql """
                EXPORT TABLE ${table_export_name} where k8 < 30
                TO "${helper.uri(outFilePath)}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "csv_with_names",
                    "column_separator"=","
                ) WITH S3 (${helper.storageProperties()});
            """
            waiting_export.call(catalog_name, ex_db_name, label)

            def export_res = sql """show export where label='${label}'"""
            logger.info("get csv_with_names export res ${export_res}")
            def outfileinfo = export_res[0][11];
            logger.info("get csv_with_names export outfile info ${outfileinfo}")

        } finally {
            helper.deleteFiles("${outFilePath}/")
        }

        // 4. csv_with_names_and_types
        uuid = UUID.randomUUID().toString()
        outFilePath = "${outfile_path_prefix}/${table_export_name}_${uuid}"
        label = "label_${uuid}"
        try {
            // exec export
            sql """
                EXPORT TABLE ${table_export_name} where k8 < 30
                TO "${helper.uri(outFilePath)}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "csv_with_names_and_types",
                    "column_separator"=","
                ) WITH S3 (${helper.storageProperties()});
            """
            waiting_export.call(catalog_name, ex_db_name, label)

            def export_res = sql """show export where label='${label}'"""
            logger.info("get csv_with_names_and_types export res ${export_res}")
            def outfileinfo = export_res[0][11];
            logger.info("get csv_with_names_and_types export outfile info ${outfileinfo}")

        } finally {
            helper.deleteFiles("${outFilePath}/")
        }

        // 5. orc
        uuid = UUID.randomUUID().toString()
        outFilePath = "${outfile_path_prefix}/${table_export_name}_${uuid}"
        label = "label_${uuid}"
        try {
            // exec export
            sql """
                EXPORT TABLE ${table_export_name} where k8 < 30
                TO "${helper.uri(outFilePath)}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "orc"
                ) WITH S3 (${helper.storageProperties()});
            """
            waiting_export.call(catalog_name, ex_db_name, label)

            def export_res = sql """show export where label='${label}'"""
            logger.info("get orc export res ${export_res}")
            def outfileinfo = export_res[0][11];
            logger.info("get orc export outfile info ${outfileinfo}")

        } finally {
            helper.deleteFiles("${outFilePath}/")
        }

        // 6. parquet
        uuid = UUID.randomUUID().toString()
        outFilePath = "${outfile_path_prefix}/${table_export_name}_${uuid}"
        label = "label_${uuid}"
        try {
            // exec export
            sql """
                EXPORT TABLE ${table_export_name} where k8 < 30
                TO "${helper.uri(outFilePath)}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "parquet"
                ) WITH S3 (${helper.storageProperties()});
            """
            waiting_export.call(catalog_name, ex_db_name, label)

            def export_res = sql """show export where label='${label}'"""
            logger.info("get parquet export res ${export_res}")
            def outfileinfo = export_res[0][11];
            logger.info("get parquet export outfile info ${outfileinfo}")

        } finally {
            helper.deleteFiles("${outFilePath}/")
        }

        // 7. test columns property
        uuid = UUID.randomUUID().toString()
        outFilePath = "${outfile_path_prefix}/${table_export_name}_${uuid}"
        label = "label_${uuid}"
        try {
            // exec export
            sql """
                EXPORT TABLE ${table_export_name} where k8 < 30
                TO "${helper.uri(outFilePath)}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "csv_with_names",
                    "columns" = "k8, k1, k5, k3, k7",
                    "column_separator"=","
                ) WITH S3 (${helper.storageProperties()});
            """
            waiting_export.call(catalog_name, ex_db_name, label)

            def export_res = sql """show export where label='${label}'"""
            logger.info("get columns property export res ${export_res}")
            def outfileinfo = export_res[0][11];
            logger.info("get columns property export outfile info ${outfileinfo}")

        } finally {
            helper.deleteFiles("${outFilePath}/")
        }
    }
}
