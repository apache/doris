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

suite("test_iceberg_hadoop_catalog_kerberos", "p0,external,kerberos,external_docker,external_docker_kerberos") {
    String enabled = context.config.otherConfigs.get("enableKerberosTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }
    def String catalog_name = "iceberg_hadoop_catalog_kerberos_test"
    def database_name = "test_iceberg_hadoop_db"
    def String test_tbl_name="iceberg_test_table"
    def keytab_root_dir = "/keytabs"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql """
            drop catalog if exists ${catalog_name}
        """
    sql """
            CREATE CATALOG IF NOT EXISTS ${catalog_name}
            PROPERTIES ( 
            'type'='iceberg',
            'iceberg.catalog.type' = 'hadoop',
            'warehouse' = 'hdfs://${externalEnvIp}:8520/tmp/iceberg/catalog',
            "hadoop.security.authentication" = "kerberos",
            "hadoop.security.auth_to_local" = "RULE:[2:\$1@\$0](.*@LABS.TERADATA.COM)s/@.*//
                                               RULE:[2:\$1@\$0](.*@OTHERLABS.TERADATA.COM)s/@.*//
                                               RULE:[2:\$1@\$0](.*@OTHERREALM.COM)s/@.*//
                                               DEFAULT",
            "hadoop.kerberos.principal" = "hive/presto-master.docker.cluster@LABS.TERADATA.COM",
            "hadoop.kerberos.min.seconds.before.relogin" = "5",
            "hadoop.kerberos.keytab.login.autorenewal.enabled" = "false",
              "hadoop.kerberos.keytab" = "${keytab_root_dir}/hive-presto-master.keytab",
            "fs.defaultFS" = "hdfs://${externalEnvIp}:8520"
            ); 
        """

    sql """ switch ${catalog_name} """
    sql """ drop database if exists ${database_name} """
    sql """ create database if not exists ${database_name} """
    def database = sql """ show databases like '%${database_name}' """
    assert database.size() == 1
    sql """ use ${database_name} """
    sql """ drop table  if exists ${test_tbl_name}"""
    sql """
       CREATE TABLE ${test_tbl_name} (                   
            `ts` DATETIME COMMENT 'ts',       
            `col1` BOOLEAN COMMENT 'col1',             
            `col2` INT COMMENT 'col2',       
            `col3` BIGINT COMMENT 'col3',           
            `col4` FLOAT COMMENT 'col4',          
            `col5` DOUBLE COMMENT 'col5',              
            `col6` DECIMAL(9,4) COMMENT 'col6',
            `col7` STRING COMMENT 'col7',                
            `col8` DATE COMMENT 'col8',             
            `col9` DATETIME COMMENT 'col9',               
            `pt1` STRING COMMENT 'pt1',                   
            `pt2` STRING COMMENT 'pt2'                
             )  ENGINE=iceberg              
                PARTITION BY LIST (DAY(ts), pt1, pt2) ()                
                 PROPERTIES (                   
                 'write-format'='orc',                
                  'compression-codec'='zlib'     
                 );
    """
    def table = sql """ show tables like '%${test_tbl_name}' """
    assert table.size() == 1
    sql """
        insert into ${test_tbl_name} values (
          '2024-05-26 12:34:56',
          true,
          123,
          1234567890123,
          12.34,
          56.789,
          12345.6789,
          'example text',
          '2024-05-26',
          '2024-05-26 14:00:00',
          'partition_val1',
          'partition_val2'
        );
    """
    def dataResult = sql """select count(1) from ${test_tbl_name} """
    assert dataResult.get(0).get(0) == 1
}
