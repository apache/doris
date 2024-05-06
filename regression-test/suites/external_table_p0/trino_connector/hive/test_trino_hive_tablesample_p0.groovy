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

suite("test_trino_hive_tablesample_p0", "all_types,p0,external,hive,external_docker,external_docker_hive") {
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



    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String hms_port = context.config.otherConfigs.get("hive2HmsPort")
            String catalog_name = "test_trino_hive_tablesample_p0"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """
                create catalog if not exists ${catalog_name} properties (
                    "type"="trino-connector",
                    "connector.name"="hive",
                    'hive.metastore.uri' = 'thrift://${externalEnvIp}:${hms_port}'
                );
            """
            sql """use `${catalog_name}`.`default`"""
            sql """SET enable_nereids_planner=true"""
            sql """SET enable_fallback_to_original_planner=false"""
            sql """select count(*) from student tablesample(10 rows);"""
            sql """select count(*) from student tablesample(10 percent);"""
            explain {
                sql("select count(*) from student tablesample(10 rows);")
                contains "count(*)[#7]"
            }
            explain {
                sql("select count(*) from student tablesample(10 percent);")
                contains "count(*)[#7]"
            }
            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}

