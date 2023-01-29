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

suite("test_hive_other", "p0") {

    def q01 = {
        qt_q24 """ select name, count(1) as c from student group by name order by c desc;"""
        qt_q25 """ select lo_orderkey, count(1) as c from lineorder group by lo_orderkey order by c desc;"""
        qt_q26 """ select * from test1 order by col_1;"""
        qt_q27 """ select * from string_table order by p_partkey desc;"""
        qt_q28 """ select * from account_fund order by batchno;"""
        qt_q29 """ select * from sale_table order by bill_code limit 01;"""
        qt_q30 """ select count(card_cnt) from hive01;"""
        qt_q31 """ select * from test2 order by id;"""
        qt_q32 """ select * from test_hive_doris order by id;"""

        order_qt_q33 """ select dt, * from table_with_vertical_line order by dt desc limit 10;"""
        order_qt_q34 """ select dt, k2 from table_with_vertical_line order by k2 desc limit 10;"""
        order_qt_q35 """ select dt, k2 from table_with_vertical_line where dt='2022-11-24' order by k2 desc limit 10;"""
        order_qt_q36 """ select k2, k5 from table_with_vertical_line where dt='2022-11-25' order by k2 desc limit 10;"""
        order_qt_q37 """ select count(*) from table_with_vertical_line;"""
        order_qt_q38 """ select k2, k5 from table_with_vertical_line where dt in ('2022-11-25') order by k2 desc limit 10;"""
        order_qt_q39 """ select k2, k5 from table_with_vertical_line where dt in ('2022-11-25', '2022-11-24') order by k2 desc limit 10;"""
        order_qt_q40 """ select dt, dt, k2, k5, dt from table_with_vertical_line where dt in ('2022-11-25') or dt in ('2022-11-25') order by k2 desc limit 10;"""
        order_qt_q41 """ select dt, dt, k2, k5, dt from table_with_vertical_line where dt in ('2022-11-25') and dt in ('2022-11-24') order by k2 desc limit 10;"""
        order_qt_q42 """ select dt, dt, k2, k5, dt from table_with_vertical_line where dt in ('2022-11-25') or dt in ('2022-11-24') order by k2 desc limit 10;"""

        order_qt_q43 """ select dt, * from table_with_x01 order by dt desc limit 10;"""
        order_qt_q44 """ select dt, k2 from table_with_x01 order by k2 desc limit 10;"""
        order_qt_q45 """ select dt, k2 from table_with_x01 where dt='2022-11-10' order by k2 desc limit 10;"""
        order_qt_q46 """ select k2, k5 from table_with_x01 where dt='2022-11-10' order by k2 desc limit 10;"""
        order_qt_q47 """ select count(*) from table_with_x01;"""
        order_qt_q48 """ select k2, k5 from table_with_x01 where dt in ('2022-11-25') order by k2 desc limit 10;"""
        order_qt_q49 """ select k2, k5 from table_with_x01 where dt in ('2022-11-10', '2022-11-10') order by k2 desc limit 10;"""
        order_qt_q50 """ select dt, dt, k2, k5, dt from table_with_x01 where dt in ('2022-11-10') or dt in ('2022-11-10') order by k2 desc limit 10;"""

        test {
            sql """select * from unsupported_type_table"""
            exception """Unsupported type 'UNSUPPORTED_TYPE'"""
        }

        qt_q51 """select * except(k4,k5) from unsupported_type_table;"""

        test {
            sql """select k1,k4 from unsupported_type_table"""
            exception """Unsupported type 'UNSUPPORTED_TYPE'"""
        }

        test {
            sql """select k1 from unsupported_type_table where k4 is null;"""
            exception """Unsupported type 'UNSUPPORTED_TYPE'"""
        }

        qt_q52 """select k1,k3,k6 from unsupported_type_table"""
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hms_port = context.config.otherConfigs.get("hms_port")
        String hdfs_port = context.config.otherConfigs.get("hdfs_port")
        String catalog_name = "hive_test_other"

        sql """drop catalog if exists ${catalog_name}"""
        sql """create resource if not exists hms_resource_hive_other properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://127.0.0.1:${hms_port}'
        );"""
        sql """create catalog if not exists ${catalog_name} with resource hms_resource_hive_other;"""

        // test user's grants on external catalog
        sql """drop user if exists ext_catalog_user"""
        sql """create user ext_catalog_user identified by '12345'"""
        sql """grant all on internal.${context.config.defaultDb}.* to ext_catalog_user"""
        sql """grant all on ${catalog_name}.*.* to ext_catalog_user"""
        connect(user = 'ext_catalog_user', password = '12345', url = context.config.jdbcUrl) {
            order_qt_ext_catalog_grants """show databases from ${catalog_name}"""
        }
        sql """drop user ext_catalog_user"""

        sql """switch ${catalog_name}"""
        sql """use `default`"""
        // order_qt_show_tables """show tables"""

        q01()

        sql """refresh catalog ${catalog_name}"""
        q01()
        sql """refresh database `default`"""
        // order_qt_show_tables2 """show tables"""
        q01()
        sql """refresh table `default`.table_with_vertical_line"""
        order_qt_after_refresh """ select dt, dt, k2, k5, dt from table_with_vertical_line where dt in ('2022-11-25') or dt in ('2022-11-24') order by k2 desc limit 10;"""

        // external table
        sql """switch internal"""
        sql """drop database if exists external_hive_table_test"""
        sql """create database external_hive_table_test"""
        sql """use external_hive_table_test"""
        sql """drop table if exists external_hive_student"""

        sql """
            create external table `external_hive_student` (
                `id` varchar(100),
                `name` varchar(100),
                `age` int,
                `gender` varchar(100),
                `addr` varchar(100),
                `phone` varchar(100)
            ) ENGINE=HIVE
            PROPERTIES
            (
                'hive.metastore.uris' = 'thrift://127.0.0.1:${hms_port}',
                'database' = 'default',
                'table' = 'student'
            );
        """
        qt_student """select * from external_hive_student order by name;"""

        // read external table
        String csv_output_dir = UUID.randomUUID().toString()
        sql """
            select * from external_hive_student
            into outfile "hdfs://127.0.0.1:${hdfs_port}/user/test/student/${csv_output_dir}/csv_"
            format as csv_with_names
            properties (
                "column_separator" = ",",
                "line_delimiter" = "\n"
            );
        """
        qt_tvf_student """
            select * from hdfs (
                "format" = "csv_with_names",
                "fs.defaultFS" = "hdfs://127.0.0.1:${hdfs_port}",
                "uri" = "hdfs://127.0.0.1:${hdfs_port}/user/test/student/${csv_output_dir}/csv_*"
            ) order by name;
        """

        sql """drop catalog if exists ${catalog_name}"""
        sql """drop resource if exists hms_resource_hive_other"""
    }
}
