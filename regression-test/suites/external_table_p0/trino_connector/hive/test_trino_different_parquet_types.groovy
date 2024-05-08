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

suite("test_trino_different_parquet_types", "p0,external,hive,external_docker,external_docker_hive") {
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




    String hms_port = context.config.otherConfigs.get("hive2HmsPort")
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    // problem 01 ：in hive execute "select * from delta_byte_array limit 10" ,there will be some valid data return，but doris query return nothing
    def q01 = {
        def res1_1 = sql """
            select * from delta_byte_array limit 10
        """ 
        logger.info("record res" + res1_1.toString())
    
        def res1_2 = sql """
            select count(*) from delta_byte_array
            """ 
            logger.info("record res" + res1_2.toString())

        def res1_3 = sql """
            select * from hdfs(\"uri" = \"hdfs://${externalEnvIp}:${hdfs_port}/user/doris/preinstalled_data/different_types_parquet/delta_byte_array/delta_byte_array.parquet\",\"format\" = \"parquet\") limit 10
            """ 
            logger.info("record res" + res1_3.toString())
    }


    // problem 2： hive query return null, doris catalog query return exception,  use tvf to query return null, but no exception

    def q03 = {

        //exception info: [INTERNAL_ERROR]Only support csv data in utf8 codec
        def res3_1 = sql """
        select * from delta_binary_packed limit 10;
        """
        logger.info("record res" + res3_1.toString())

        def res3_2 = sql """
        select count(*) from delta_binary_packed;
        """
        logger.info("record res" + res3_1.toString())

        //return nothing,but no exception
        def res3_3 = sql """
            select * from hdfs(\"uri" = \"hdfs://${externalEnvIp}:${hdfs_port}/user/doris/preinstalled_data/different_types_parquet/delta_binary_packed/delta_binary_packed.parquet\",\"format\" = \"parquet\") limit 10
            """ 
            logger.info("record res" + res3_3.toString())
    }

    //problem 3： hive query exception, doris query return nothing
    def q04 = {
        def res4_1 = sql """
        select * from delta_encoding_required_column limit 10;
        """
        logger.info("record res" + res4_1.toString())

        def res4_2 = sql """
        select count(*) from delta_encoding_required_column;
        """
        logger.info("record res" + res4_2.toString())

        def res4_3 = sql """
             select * from hdfs(\"uri" = \"hdfs://${externalEnvIp}:${hdfs_port}/user/doris/preinstalled_data/different_types_parquet/delta_encoding_required_column/delta_encoding_required_column.parquet\",\"format\" = \"parquet\") limit 10
             """ 
        logger.info("record res" + res4_3.toString())
    }


    def q05 = {
        def res5_1 = sql """
        select * from delta_encoding_optional_column limit 10;
        """
        logger.info("record res" + res5_1.toString())


        def res5_2 = sql """
        select count(*) from delta_encoding_optional_column;
        """
        logger.info("record res" + res5_2.toString())

         def res5_3 = sql """
        select * from hdfs(\"uri" = \"hdfs://${externalEnvIp}:${hdfs_port}/user/doris/preinstalled_data/different_types_parquet/delta_encoding_optional_column/delta_encoding_optional_column.parquet\",\"format\" = \"parquet\") limit 10
        """ 
        logger.info("record res" + res5_3.toString())
    }


    // problem 4：tvf query exception:  Can not get first file, please check uri.
    def q06 = {
        def res6_1 = sql """
        select * from datapage_v1_snappy_compressed_checksum limit 10;
        """
        logger.info("record res" + res6_1.toString())

        def res6_2 = sql """
        select count(*) from datapage_v1_snappy_compressed_checksum;
        """
        logger.info("record res" + res6_2.toString())

        def res6_3 = sql """
        select * from hdfs(\"uri" = \"hdfs://${externalEnvIp}:${hdfs_port}/user/doris/preinstalled_data/different_types_parquet/datapage_v1-snappy-compressed-checksum/datapage_v1-snappy-compressed-checksum.parquet\",\"format\" = \"parquet\") limit 10
        """ 
        logger.info("record res" + res6_3.toString())

    }

    //pass
    def q07 = {   
        def res7_1 = sql """
        select * from overflow_i16_page_cnt limit 10;
    """
        logger.info("record res" + res7_1.toString())

        def res7_2 = sql """
        select count(*) from overflow_i16_page_cnt;
    """
        logger.info("record res" + res7_2.toString())

         def res7_3 = sql """
        select * from hdfs(\"uri" = \"hdfs://${externalEnvIp}:${hdfs_port}/user/doris/preinstalled_data/different_types_parquet/overflow_i16_page_cnt/overflow_i16_page_cnt.parquet\",\"format\" = \"parquet\") limit 10
        """ 
        logger.info("record res" + res7_3.toString())
    }

    //pass
    def q08 = {
        def res8_1 = sql """
        select * from alltypes_tiny_pages limit 10;
    """
        logger.info("record res" + res8_1.toString())


        def res8_2 = sql """
        select count(*) from alltypes_tiny_pages limit 10;
    """
        logger.info("record res" + res8_2.toString())

        def res8_3 = sql """
        select * from hdfs(\"uri" = \"hdfs://${externalEnvIp}:${hdfs_port}/user/doris/preinstalled_data/different_types_parquet/alltypes_tiny_pages/alltypes_tiny_pages.parquet\",\"format\" = \"parquet\") limit 10
        """ 
        logger.info("record res" + res8_3.toString())
    }
    //pass
    def q09 = {
        def res9_1 = sql """
        select * from alltypes_tiny_pages_plain limit 10;
    """
        logger.info("record res" + res9_1.toString())


        def res9_2 = sql """
        select count(*) from alltypes_tiny_pages_plain limit 10;
    """
        logger.info("record res" + res9_2.toString())

         def res9_3 = sql """
        select * from hdfs(\"uri" = \"hdfs://${externalEnvIp}:${hdfs_port}/user/doris/preinstalled_data/different_types_parquet/alltypes_tiny_pages_plain/alltypes_tiny_pages_plain.parquet\",\"format\" = \"parquet\") limit 10
        """ 
        logger.info("record res" + res9_3.toString())
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String catalog_name = "test_trino_different_parquet_types"
            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="trino-connector",
                "connector.name"="hive",
                'hive.metastore.uri' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`default`"""

            q01()
            // q02()
            q03()
            q04()
            q05()
            q06()
            q07()
            q08()
            q09()
            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
