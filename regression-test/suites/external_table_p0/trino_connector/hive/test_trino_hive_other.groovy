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

suite("test_trino_hive_other", "external,hive,external_docker,external_docker_hive") {
    def trino_connector_download_dir = context.config.otherConfigs.get("trinoPluginsPath")

    // mkdir trino_connector_download_dir
    logger.info("start create dir ${trino_connector_download_dir} ...")
    def mkdir_connectors_tar = "mkdir -p ${trino_connector_download_dir}".execute().getText()
    logger.info("finish create dir, result: ${mkdir_connectors_tar} ...")


    def plugins_compression =  "${trino_connector_download_dir}/trino-connectors.tar.gz"
    def plugins_dir = "${trino_connector_download_dir}/connectors"

    // download trino-connectors.tar.gz
    File path = new File("${plugins_compression}")
    if (path.exists() && path.isFile()) {
        logger.info("${plugins_compression} has been downloaded")
    } else {
        logger.info("start delete trino-connector plugins dir ...")
        def delete_local_connectors_tar = "rm -r ${plugins_dir}".execute()
        logger.info("start download trino-connector plugins ...")
        def s3_url = getS3Url()

        logger.info("getS3Url ==== ${s3_url}")
        def download_connectors_tar = "/usr/bin/curl ${s3_url}/regression/trino-connectors.tar.gz --output ${plugins_compression}"
        logger.info("download cmd : ${download_connectors_tar}")
        def run_download_connectors_cmd = download_connectors_tar.execute().getText()
        logger.info("result: ${run_download_connectors_cmd}")
        logger.info("finish download ${plugins_compression} ...")
    }

    // decompression trino-plugins.tar.gz
    File dir = new File("${plugins_dir}")
    if (dir.exists() && dir.isDirectory()) {
        logger.info("${plugins_dir} dir has been decompressed")
    } else {
        if (path.exists() && path.isFile()) {
            def run_cmd = "tar -zxvf ${plugins_compression} -C ${trino_connector_download_dir}"
            logger.info("run_cmd : $run_cmd")
            def run_decompress_cmd = run_cmd.execute().getText()
            logger.info("result: $run_decompress_cmd")
        } else {
            logger.info("${plugins_compression} is not exist or is not a file.")
            throw exception
        }
    }


    def q01 = {
        qt_q24 """ select name, count(1) as c from student group by name order by name desc;"""
        qt_q25 """ select lo_orderkey, count(1) as c from lineorder group by lo_orderkey order by lo_orderkey asc, c desc;"""
        qt_q26 """ select * from test1 order by col_1;"""
        qt_q27 """ select * from string_table order by p_partkey desc;"""
        qt_q28 """ select * from account_fund order by batchno;"""
        qt_q29 """ select * from sale_table order by bill_code limit 01;"""
        order_qt_q30 """ select count(card_cnt) from hive01;"""
        qt_q31 """ select * from test2 order by id;"""
        qt_q32 """ select * from test_hive_doris order by id;"""

        qt_q33 """ select dt, k1, * from table_with_vertical_line order by dt desc, k1 desc limit 10;"""
        order_qt_q34 """ select dt, k2 from table_with_vertical_line order by k2 desc limit 10;"""
        qt_q35 """ select dt, k2 from table_with_vertical_line where dt='2022-11-24' order by k2 desc limit 10;"""
        qt_q36 """ select k2, k5 from table_with_vertical_line where dt='2022-11-25' order by k2 desc limit 10;"""
        order_qt_q37 """ select count(*) from table_with_vertical_line;"""
        qt_q38 """ select k2, k5 from table_with_vertical_line where dt in ('2022-11-25') order by k2 desc limit 10;"""
        qt_q39 """ select k2, k5 from table_with_vertical_line where dt in ('2022-11-25', '2022-11-24') order by k2 desc limit 10;"""
        qt_q40 """ select dt, dt, k2, k5, dt from table_with_vertical_line where dt in ('2022-11-25') or dt in ('2022-11-25') order by k2 desc limit 10;"""
        qt_q41 """ select dt, dt, k2, k5, dt from table_with_vertical_line where dt in ('2022-11-25') and dt in ('2022-11-24') order by k2 desc limit 10;"""
        qt_q42 """ select dt, dt, k2, k5, dt from table_with_vertical_line where dt in ('2022-11-25') or dt in ('2022-11-24') order by k2 desc limit 10;"""

        qt_q43 """ select dt, k1, * from table_with_x01 order by dt desc, k1 desc limit 10;"""
        qt_q44 """ select dt, k2 from table_with_x01 order by k2 desc limit 10;"""
        qt_q45 """ select dt, k2 from table_with_x01 where dt='2022-11-10' order by k2 desc limit 10;"""
        qt_q46 """ select k2, k5 from table_with_x01 where dt='2022-11-10' order by k2 desc limit 10;"""
	order_qt_q47 """ select count(*) from table_with_x01;"""
        qt_q48 """ select k2, k5 from table_with_x01 where dt in ('2022-11-25') order by k2 desc limit 10;"""
        qt_q49 """ select k2, k5 from table_with_x01 where dt in ('2022-11-10', '2022-11-10') order by k2 desc limit 10;"""
        qt_q50 """ select dt, dt, k2, k5, dt from table_with_x01 where dt in ('2022-11-10') or dt in ('2022-11-10') order by k2 desc limit 10;"""
        qt_q51 """ select col_2 from test1;"""
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hms_port = context.config.otherConfigs.get("hive2HmsPort")
        String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        String catalog_name = "test_trino_hive_other"

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="trino-connector",
            "connector.name"="hive",
            'hive.metastore.uri' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""

        // test user's grants on external catalog
        sql """drop user if exists ext_catalog_user"""
        sql """create user ext_catalog_user identified by '12345'"""
        sql """grant all on internal.${context.config.defaultDb}.* to ext_catalog_user"""
        sql """grant all on ${catalog_name}.*.* to ext_catalog_user"""
        connect(user = 'ext_catalog_user', password = '12345', url = context.config.jdbcUrl) {
            def database_lists = sql """show databases from ${catalog_name}"""
            boolean ok = false;
            for (int i = 0; i < database_lists.size(); ++j) {
                assertEquals(1, database_lists[i].size())
                if (database_lists[i][0].equals("default")) {
                    ok = true;
                    break;
                }
            }
            if (!ok) {
                throw exception
            }
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

        sql """drop catalog if exists ${catalog_name}"""
    }
}
