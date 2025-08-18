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
import static groovy.test.GroovyAssert.shouldFail;

suite("iceberg_and_hive_on_glue", "p0,external,hive,external_docker,external_docker_hive") {


    def testQueryAndInsert = { String catalogProperties, String prefix, String dbLocation ->

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

        def db_name = prefix + "_db" + System.currentTimeMillis()
        sql """
            DROP DATABASE IF EXISTS ${db_name} FORCE;
        """
        sql """
            CREATE DATABASE IF NOT EXISTS ${db_name}
            PROPERTIES ('location'='${dbLocation}');

        """

        def dbResult = sql """
            show databases  like "${db_name}";
        """
        assert dbResult.size() == 1

        sql """
            use ${db_name};
        """
        def table_name = prefix + "_table"
        sql """
            CREATE TABLE ${table_name} (
            user_id            BIGINT      COMMENT "user id",
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

    def testQueryAndInsertIcerberg = { String catalogProperties, String prefix ->

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

    /*--------only execute query---------*/
    def testQuery = { String catalog_properties, String prefix, String db_name, String table_name, int data_count ->

        def catalog_name = "${prefix}_catalog"
        sql """
            DROP CATALOG IF EXISTS ${catalog_name};
        """
        sql """
            CREATE CATALOG IF NOT EXISTS ${catalog_name} PROPERTIES (
                ${catalog_properties}
            );
        """
        sql """
            switch ${catalog_name};
        """


        def dbResult = sql """
            show databases  like "${db_name}";
        """
        assert dbResult.size() == 1

        sql """
            use ${db_name};
        """

        // query
        def queryResult = sql """
            SELECT count(1) FROM ${table_name};
        """
        assert queryResult.get(0).get(0) == data_count
    }

    /*--------GLUE START-----------*/
    String s3_ak = context.config.otherConfigs.get("AWSAK")
    String s3_sk = context.config.otherConfigs.get("AWSSK")
    String s3_endpoint = "http://s3.ap-east-1.amazonaws.com/"
    String s3_region = "ap-east-1"
    String s3_warehouse = "s3://selectdb-qa-datalake-test-hk/iceberg-glue/"


    String s3_storage_properties = """
      's3.access_key' = '${s3_ak}',
      's3.secret_key' = '${s3_sk}',
      's3.endpoint' = '${s3_endpoint}'
    """
    String iceberg_glue_catalog_base_properties = """

    'type'='iceberg',
    'iceberg.catalog.type' = 'glue',
    """
    String hms_glue_catalog_base_properties = """
    'type'='hms',
    'hive.metastore.type'='glue',
    """
    String glue_properties_1 = """

    'glue.endpoint' = 'https://glue.${s3_region}.amazonaws.com',
    'client.credentials-provider.glue.access_key' = '${s3_ak}',
    'client.credentials-provider.glue.secret_key' = '${s3_sk}'
    """
    String glue_properties_2 = """
   
    'glue.endpoint' = 'https://glue.${s3_region}.amazonaws.com',
    'glue.access_key' = '${s3_ak}',
    'glue.secret_key' = '${s3_sk}'
    """
    String glue_properties_3 = """
   
    'glue.endpoint' = 'https://glue.${s3_region}.amazonaws.com',
    'glue.access_key' = '${s3_ak}',
    'glue.secret_key' = '${s3_sk}',
     ${s3_storage_properties}
    """
    String warehouse_location = """
      'warehouse'='${s3_warehouse}hive-glue-s3-warehouse/iceberg-glue/',
    """
    testQueryAndInsertIcerberg(warehouse_location + iceberg_glue_catalog_base_properties + glue_properties_1, "iceberg_glue_on_s3")
    testQueryAndInsertIcerberg(warehouse_location + iceberg_glue_catalog_base_properties + glue_properties_2, "iceberg_glue_on_s3")
    testQueryAndInsertIcerberg(warehouse_location + iceberg_glue_catalog_base_properties + glue_properties_3, "iceberg_glue_on_s3")
}