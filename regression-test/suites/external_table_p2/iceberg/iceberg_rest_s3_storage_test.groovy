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

suite("iceberg_rest_s3_storage_test", "p2,external,iceberg,external_docker,external_docker_iceberg_rest,new_catalog_property") {

    def testQueryAndInsert = { String catalogProperties, String prefix ->

        // =======  BASIC CATALOG AND DATABASE SETUP  =======
        def catalog_name = "${prefix}_catalog"
        sql """
            DROP CATALOG IF EXISTS ${catalog_name};
        """
        sql """
            CREATE CATALOG IF NOT EXISTS ${catalog_name} PROPERTIES (
                ${catalogProperties}
            );
        """
        sql """
            switch ${catalog_name};
        """

        def db_name = prefix + "_db"
        sql """
            DROP DATABASE IF EXISTS ${db_name} FORCE;
        """
        sql """
            CREATE DATABASE IF NOT EXISTS ${db_name};
        """

        def dbResult = sql """
            show databases  like "${db_name}";
        """
        assert dbResult.size() == 1

        sql """
            use ${db_name};
        """
        // =======  BASIC TABLE OPERATIONS TEST  =======
        def table_name = prefix + "_table"
        sql """
            CREATE TABLE ${table_name} (
            user_id            BIGINT       NOT NULL COMMENT "user id",
            name               VARCHAR(20)           COMMENT "name",
            age                INT                   COMMENT "age"
        );
        """
        sql """
            insert into ${table_name} values (1, 'a', 10);
        """
        // query
        def queryResult = sql """
            SELECT * FROM ${table_name};
        """
        assert queryResult.size() == 1

        // =======  BRANCH/TAG TEST  =======
        def branch_name = prefix + "_branch"
        def tag_name = prefix + "_tag"
        sql """
            ALTER TABLE ${table_name} CREATE BRANCH ${branch_name};
        """
        sql """
            ALTER TABLE ${table_name} CREATE TAG ${tag_name};
        """
        sql """
            INSERT OVERWRITE TABLE ${table_name} VALUES (1, 'a', 10),(2, 'b', 20), (3, 'c', 30)
        """
        def originalQueryResult = sql """
            SELECT * FROM ${table_name};
        """
        assert originalQueryResult.size() == 3
        sql """
            insert into ${table_name}@branch(${branch_name}) values (4, 'd', 40)
        """
        def branchQueryResult = sql """
            SELECT * FROM ${table_name}@branch(${branch_name});
        """
        assert branchQueryResult.size() == 2


        def tagQueryResult = sql """
            SELECT * FROM ${table_name}@tag(${tag_name});
        """
        assert tagQueryResult.size() == 1

        // Note: Tags are read-only in Iceberg, only branches support write operations

        sql """
            ALTER TABLE ${table_name} drop branch ${branch_name};
        """
        sql """
            ALTER TABLE ${table_name} drop tag ${tag_name};
        """
        // =======  SYSTEM TABLES TEST  =======
        // Test $files system table
        def files_result = sql """
        SELECT * FROM ${table_name}\$files;
    """
        println "Files system table: " + files_result

        // Test $entries system table
        def entries_result = sql """
        SELECT * FROM ${table_name}\$entries;
    """
        println "Entries system table: " + entries_result

        // Test $history system table
        def history_result = sql """
        SELECT * FROM ${table_name}\$history;
    """
        println "History system table: " + history_result

        // Test $manifests system table
        def manifests_result = sql """
        SELECT * FROM ${table_name}\$manifests;
    """
        println "Manifests system table: " + manifests_result

        // Test $metadata_log_entries system table
        def metadata_log_result = sql """
        SELECT * FROM ${table_name}\$metadata_log_entries;
    """
        println "Metadata log entries system table: " + metadata_log_result

        // Test $partitions system table  
        def partitions_result = sql """
        SELECT * FROM ${table_name}\$partitions;
    """
        println "Partitions system table: " + partitions_result

        // Test $refs system table
        def refs_result = sql """
        SELECT * FROM ${table_name}\$refs;
    """
        println "Refs system table: " + refs_result

        // Test $snapshots system table
        def snapshots_result = sql """
        SELECT * FROM ${table_name}\$snapshots;
    """
        println "Snapshots system table: " + snapshots_result

        println "All system tables test SUCCESS " + catalog_name

        // =======  TIME TRAVEL TEST  =======
        def iceberg_meta_result = sql """
        SELECT snapshot_id FROM iceberg_meta(
                'table' = '${catalog_name}.${db_name}.${table_name}',
                'query_type' = 'snapshots'
        ) order by committed_at desc;
        """
        def first_snapshot_id = iceberg_meta_result.get(0).get(0);
        def time_travel = sql """
        SELECT * FROM ${table_name} FOR VERSION AS OF ${first_snapshot_id};
        """
        println time_travel
        println "iceberg_time_travel_QUERY SUCCESS " + catalog_name


        sql """
            DROP TABLE ${table_name};
        """

        // =======  PARTITION TABLE TEST  =======
        table_name = prefix + "_partition_table"
        sql """
            CREATE TABLE ${table_name} (
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
            )
            PARTITION BY LIST (day(ts), pt1, pt2) ()
            PROPERTIES (
              'write-format'='orc',
              'compression-codec'='zlib'
            );
        """

        sql """
            INSERT OVERWRITE  TABLE ${table_name} values 
            ('2023-01-01 00:00:00', true, 1, 1, 1.0, 1.0, 1.0000, '1', '2023-01-01', '2023-01-01 00:00:00', 'a', '1'),
            ('2023-01-02 00:00:00', false, 2, 2, 2.0, 2.0, 2.0000, '2', '2023-01-02', '2023-01-02 00:00:00', 'b', '2'),
            ('2023-01-03 00:00:00', true, 3, 3, 3.0, 3.0, 3.0000, '3', '2023-01-03', '2023-01-03 00:00:00', 'c', '3');
        """
        def partitionQueryResult = sql """
            SELECT * FROM ${table_name} WHERE pt1='a' and pt2='1';
        """
        assert partitionQueryResult.size() == 1

        // =======  PARTITION TABLE BRANCH/TAG TEST  =======
        branch_name = prefix + "_partition_branch"
        tag_name = prefix + "_partition_tag"

        sql """
            ALTER TABLE ${table_name} CREATE BRANCH ${branch_name};
        """
        sql """
            ALTER TABLE ${table_name} CREATE TAG ${tag_name};
        """

        // Partition table branch write operation
        sql """
            insert into ${table_name}@branch(${branch_name}) values ('2023-01-04 00:00:00', false, 4, 4, 4.0, 4.0, 4.0000, '4', '2023-01-04', '2023-01-04 00:00:00', 'd', '4')
        """

        def partitionBranchResult = sql """
            SELECT * FROM ${table_name}@branch(${branch_name}) ORDER BY col2;
        """
        println "Partition table branch query: " + partitionBranchResult

        def partitionTagResult = sql """
            SELECT * FROM ${table_name}@tag(${tag_name}) ORDER BY col2;
        """
        println "Partition table tag query: " + partitionTagResult

        // Test partition table system tables
        def partition_files_result = sql """
            SELECT * FROM ${table_name}\$partitions;
        """
        println "Partitions system table: " + partition_files_result

        sql """
            ALTER TABLE ${table_name} drop branch ${branch_name};
        """
        sql """
            ALTER TABLE ${table_name} drop tag ${tag_name};
        """

        sql """
            DROP TABLE ${table_name};
        """

        sql """
            DROP DATABASE ${db_name} FORCE;
        """

        def dropResult = sql """
            show databases  like "${db_name}";
        """
        assert dropResult.size() == 0
    }

    String enabled = context.config.otherConfigs.get("enableExternalIcebergTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        /* REST catalog env and base properties */
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        String rest_port_s3 = context.config.otherConfigs.get("iceberg_rest_uri_port_s3")
        String iceberg_rest_type_prop_s3 = """
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'iceberg.rest.uri' = 'http://${externalEnvIp}:${rest_port_s3}',
        """

        String rest_port_oss = context.config.otherConfigs.get("iceberg_rest_uri_port_oss")
        String iceberg_rest_type_prop_oss = """
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'iceberg.rest.uri' = 'http://${externalEnvIp}:${rest_port_oss}',
        """

        String rest_port_cos = context.config.otherConfigs.get("iceberg_rest_uri_port_cos")
        String iceberg_rest_type_prop_cos = """
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'iceberg.rest.uri' = 'http://${externalEnvIp}:${rest_port_cos}',
        """

        /*-----S3------*/
        String s3_ak = context.config.otherConfigs.get("AWSAk")
        String s3_sk = context.config.otherConfigs.get("AWSSk")
        String s3_parent_path = "selectdb-qa-datalake-test-hk"
        String s3_endpoint = "https://s3.ap-east-1.amazonaws.com"
        String s3_region = "ap-east-1"
        String s3_storage_properties = """
          's3.access_key' = '${s3_ak}',
          's3.secret_key' = '${s3_sk}',
          's3.endpoint' = '${s3_endpoint}'
        """
        String s3_region_param = """
         's3.region' = '${s3_region}',
        """
        /****************OSS*******************/
        String oss_ak = context.config.otherConfigs.get("aliYunAk")
        String oss_sk = context.config.otherConfigs.get("aliYunSk")
        String endpoint = context.config.otherConfigs.get("aliYunEndpoint")
        String oss_endpoint = "https://${endpoint}"
        String oss_parent_path = context.config.otherConfigs.get("aliYunBucket")
        String oss_region = context.config.otherConfigs.get("aliYunRegion")

        String oss_region_param = """
         'oss.region' = '${oss_region}',
        """
        String oss_storage_properties = """
          'oss.access_key' = '${oss_ak}',
          'oss.secret_key' = '${oss_sk}',
          'oss.endpoint' = '${oss_endpoint}'
        """
        /****************COS*******************/
        String cos_ak = context.config.otherConfigs.get("txYunAk")
        String cos_sk = context.config.otherConfigs.get("txYunSk")
        String cos_parent_path = "sdb-qa-datalake-test-1308700295";
        String cos_endpoint = "https://cos.ap-beijing.myqcloud.com"
        String cos_region = "ap-beijing"
        String cos_region_param = """
         'cos.region' = '${cos_region}',
        """


        String cos_storage_properties = """
          'cos.access_key' = '${cos_ak}',
          'cos.secret_key' = '${cos_sk}',
          'cos.endpoint' = '${cos_endpoint}'
        """

        // -------- REST on OSS --------
        String warehouse = """
         'warehouse' = 's3://${oss_parent_path}/iceberg_rest_warehouse',
        """
        testQueryAndInsert(iceberg_rest_type_prop_oss + warehouse + oss_storage_properties, "iceberg_rest_on_oss")
        testQueryAndInsert(iceberg_rest_type_prop_oss + warehouse + oss_region_param + oss_storage_properties, "iceberg_rest_on_oss_region")

        // -------- REST on COS --------
        warehouse = """
         'warehouse' = 's3://${cos_parent_path}/iceberg_rest_warehouse',
        """
        testQueryAndInsert(iceberg_rest_type_prop_cos + warehouse + cos_storage_properties, "iceberg_rest_on_cos")
        testQueryAndInsert(iceberg_rest_type_prop_cos + warehouse + cos_region_param + cos_storage_properties, "iceberg_rest_on_cos_region")

        // -------- REST on S3 --------
        warehouse = """
         'warehouse' = 's3://${s3_parent_path}/iceberg_rest_warehouse',
        """
        testQueryAndInsert(iceberg_rest_type_prop_s3 + warehouse + s3_storage_properties, "iceberg_rest_on_s3")
        testQueryAndInsert(iceberg_rest_type_prop_s3 + warehouse + s3_region_param + s3_storage_properties, "iceberg_rest_on_s3_region")
        warehouse = """
         'warehouse' = 's3a://${s3_parent_path}/iceberg_rest_warehouse',
        """
        testQueryAndInsert(iceberg_rest_type_prop_s3 + warehouse + s3_storage_properties, "iceberg_rest_on_s3a")
        testQueryAndInsert(iceberg_rest_type_prop_s3 + warehouse + s3_region_param + s3_storage_properties, "iceberg_rest_on_s3a_region")
    }
}