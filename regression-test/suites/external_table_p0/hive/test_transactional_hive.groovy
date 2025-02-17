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

suite("test_transactional_hive", "p0,external,hive,external_docker,external_docker_hive") {
    String skip_checking_acid_version_file = "false"

    def q01 = {
        sql """set skip_checking_acid_version_file=${skip_checking_acid_version_file}"""
        qt_q01 """
        select * from orc_full_acid order by id;
        """
        qt_q02 """
        select value from orc_full_acid order by id;
        """
        qt_q04 """
        select * from orc_full_acid_empty;
        """
        qt_q05 """
        select count(*) from orc_full_acid_empty;
        """
    }

    def q01_par = {
        sql """set skip_checking_acid_version_file=${skip_checking_acid_version_file}"""
        qt_q01 """
        select * from orc_full_acid_par order by id;
        """
        qt_q02 """
        select value from orc_full_acid_par order by id;
        """
        qt_q03 """
        select * from orc_full_acid_par where value = 'BB' order by id;
        """
        qt_q04 """
        select * from orc_full_acid_par_empty;
        """
        qt_q05 """
        select count(*) from orc_full_acid_par_empty;
        """
    }


    def test_acid = {
        try {
            sql """SET enable_fallback_to_original_planner=false;"""
            sql """ select * from orc_to_acid_tb """ 
        }catch( Exception e) {
            logger.info("e.getMessage()" + e.getMessage())
            assertTrue(e.getMessage().contains("Original non-ACID files in transactional tables are not supported"));
        }

        qt_2 """ select * from orc_to_acid_compacted_tb order by id """
        qt_3 """ select * from orc_to_acid_compacted_tb where part_col=101 order by id """
        qt_4 """ select * from orc_to_acid_compacted_tb where part_col=102 order by id """
        qt_5 """ select * from orc_to_acid_compacted_tb where id < 3 order by id """
        qt_6 """ select * from orc_to_acid_compacted_tb where id > 3 order by id """


        qt_7 """ select * from orc_acid_minor order by id """
        qt_10 """ select * from orc_acid_minor where id < 3 order by id """
        qt_11 """ select * from orc_acid_minor where id > 3 order by id """


        qt_12 """ select * from orc_acid_major order by id """
        qt_15 """ select * from orc_acid_major where id < 3 order by id """
        qt_16 """ select * from orc_acid_major where id > 3 order by id """
    }

    def test_acid_write = {
        sql """set enable_fallback_to_original_planner=false;"""



        try {
            sql """ 
                CREATE TABLE acid_tb (
                `col1` BOOLEAN COMMENT 'col1',
                `col2` INT COMMENT 'col2'
                )  ENGINE=hive
                PROPERTIES (
                'file_format'='orc',
                'compression'='zlib',
                'bucketing_version'='2',
                'transactional'='true',
                'transactional_properties'='default'
                );
            """
        }catch( Exception e) {
            assertTrue(e.getMessage().contains("Not support create hive transactional table."));
        }
        try { 
            sql """ insert into orc_acid_major(id,value) values(1,"a1"); """
        }catch (Exception e) {
            assertTrue(e.getMessage().contains("Not supported insert into hive transactional table."));
        }

        try { 
            sql """ drop table orc_acid_major; """
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Not support drop hive transactional table."));

        }
    }


    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive3"]) {
        try {
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "test_transactional_${hivePrefix}"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
                ,'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
            );"""
            sql """use `${catalog_name}`.`default`"""

            skip_checking_acid_version_file = "false"
            q01()
            q01_par()

            skip_checking_acid_version_file = "true"
            q01()
            q01_par()

            test_acid()
            test_acid_write()

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
