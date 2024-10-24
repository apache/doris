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

import java.net.URLClassLoader
import java.sql.Connection
import java.sql.DriverManager
import java.net.URL

suite("test_db2_jdbc_catalog", "p0,external,db2,external_docker,external_docker_db2") {
    qt_sql """select current_catalog()"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/jcc-11.5.8.0.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "db2_jdbc_catalog";
        String internal_db_name = "regression_test_jdbc_catalog_p0";
        String ex_db_name = "DORIS_TEST";
        String db2_port = context.config.otherConfigs.get("db2_11_port");
        String sample_table = "SAMPLE_TABLE";

        try {
            db2_docker "CREATE SCHEMA doris_test;"
            db2_docker "CREATE SCHEMA test;"
            db2_docker """CREATE TABLE doris_test.sample_table (
                id_column INT GENERATED ALWAYS AS IDENTITY,
                numeric_column NUMERIC,
                decimal_column DECIMAL(31, 10),
                decfloat_column DECFLOAT,
                float_column FLOAT,
                real_column REAL,
                double_column DOUBLE,
                double_precision_column DOUBLE PRECISION,
                smallint_column SMALLINT,
                int_column INT,
                bigint_column BIGINT,
                varchar_column VARCHAR(255),
                varcharphic_column VARGRAPHIC(50),
                long_varchar_column LONG VARCHAR,
                long_varcharphic_column LONG VARGRAPHIC,
                char_varying_column CHAR VARYING(255),
                char_column CHAR(255),
                date_column DATE,
                timestamp_column TIMESTAMP,
                time_column TIME,
                clob_column CLOB
            );"""

            db2_docker """INSERT INTO doris_test.sample_table (
                numeric_column,
                decimal_column,
                decfloat_column,
                float_column,
                real_column,
                double_column,
                double_precision_column,
                smallint_column,
                int_column,
                bigint_column,
                varchar_column,
                varcharphic_column,
                long_varchar_column,
                long_varcharphic_column,
                char_varying_column,
                char_column,
                date_column,
                timestamp_column,
                time_column,
                clob_column
            ) VALUES (
                123,
                1234567890.1234567890,
                1.234567890,
                12345.6789,
                12345.6789,
                1234567890.1234567890,
                1234567890.1234567890,
                123,
                12345,
                123456789012345,
                'Varchar text',
                'Varcharphic text',
                'Long varchar text',
                'Long varcharphic text',
                'Char varying text',
                'Char text',
                '2024-01-24',
                '2024-01-24-12.34.56.789000',
                '12:34:56',
                'Sample CLOB text'
            );"""

            db2_docker """INSERT INTO doris_test.sample_table (
                numeric_column,
                decimal_column,
                decfloat_column,
                float_column,
                real_column,
                double_column,
                double_precision_column,
                smallint_column,
                int_column,
                bigint_column,
                varchar_column,
                varcharphic_column,
                long_varchar_column,
                long_varcharphic_column,
                char_varying_column,
                char_column,
                date_column,
                timestamp_column,
                time_column,
                clob_column
            ) VALUES (
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            );"""

            db2_docker """INSERT INTO doris_test.sample_table (
                numeric_column,
                decimal_column,
                decfloat_column,
                float_column,
                real_column,
                double_column,
                double_precision_column,
                smallint_column,
                int_column,
                bigint_column,
                varchar_column,
                varcharphic_column,
                long_varchar_column,
                long_varcharphic_column,
                char_varying_column,
                char_column,
                date_column,
                timestamp_column,
                time_column,
                clob_column
            ) VALUES (
                123,
                1234567890.1234567890,
                1.234567890,
                12345.6789,
                12345.6789,
                1234567890.1234567890,
                1234567890.1234567890,
                123,
                12345,
                123456789012345,
                '中文一',
                '中文二',
                '中文三',
                '中文四',
                '中文五',
                '中文六',
                '2024-01-24',
                '2024-01-24-12.34.56.789000',
                '12:34:56',
                '中文七'
            );"""

            sql """create database if not exists ${internal_db_name}; """

            sql """drop catalog if exists ${catalog_name} """

            sql """create catalog if not exists ${catalog_name} properties(
                "type"="jdbc",
                "user"="db2inst1",
                "password"="123456",
                "jdbc_url" = "jdbc:db2://${externalEnvIp}:${db2_port}/doris:allowNextOnExhaustedResultSet=1;resultSetHoldability=1",
                "driver_url" = "${driver_url}",
                "driver_class" = "com.ibm.db2.jcc.DB2Driver"
            );"""

            sql """switch ${catalog_name}"""
            qt_sql """select current_catalog()"""
            sql """ use ${ex_db_name}"""

            order_qt_sample_table_desc """ desc ${sample_table}; """

            order_qt_sample_table_select  """ select * except(ID_COLUMN) from ${sample_table} order by 1; """

            sql """INSERT INTO ${sample_table} (
                       numeric_column,
                       decimal_column,
                       decfloat_column,
                       float_column,
                       real_column,
                       double_column,
                       double_precision_column,
                       smallint_column,
                       int_column,
                       bigint_column,
                       varchar_column,
                       varcharphic_column,
                       long_varchar_column,
                       long_varcharphic_column,
                       char_varying_column,
                       char_column,
                       date_column,
                       timestamp_column,
                       time_column,
                       clob_column
                   )
                   select * except(ID_COLUMN) from ${sample_table};
            """

            db2_docker """create table test.books(id bigint not null
                          primary key, book XML);"""

            db2_docker """insert into test.books values(1000, '<catalog>
                          <book>

                          <author> Gambardella Matthew</author>
                          <title>XML Developers Guide</title>
                          <genre>Computer</genre>
                          <price>44.95</price>
                          <publish_date>2000-10-01</publish_date>
                          <description>An in-depth look at creating application
                          with XML</description>
                          </book>

                          </catalog>');"""

            order_qt_sample_table_insert  """ select * except(ID_COLUMN) from ${sample_table} order by 1; """

            order_qt_desc_db "show databases from ${catalog_name};"

            order_qt_select_xml "select * from TEST.BOOKS;"

            sql """ drop catalog if exists ${catalog_name} """

            db2_docker "DROP TABLE IF EXISTS doris_test.sample_table;"
            db2_docker "DROP SCHEMA doris_test restrict;"
            db2_docker "DROP TABLE IF EXISTS test.books;"
            db2_docker "DROP SCHEMA test restrict;"

        } catch (Exception e) {
            e.printStackTrace()
        }
    }
}
