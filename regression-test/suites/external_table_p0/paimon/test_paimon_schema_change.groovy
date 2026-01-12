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

suite("test_paimon_schema_change", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
        String catalog_name = "test_paimon_schema_change"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        
        String table_name = "ts_scale_orc"

        sql """drop catalog if exists ${catalog_name}"""

        sql """
            CREATE CATALOG ${catalog_name} PROPERTIES (
                    'type' = 'paimon',
                    'warehouse' = 's3://warehouse/wh',
                    's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
                    's3.access_key' = 'admin',
                    's3.secret_key' = 'password',
                    's3.path.style.access' = 'true'
            );
        """
        sql """switch `${catalog_name}`"""
        sql """show databases; """
        sql """use test_paimon_schema_change """ 


        qt_desc_1  """ desc sc_parquet_pk """

        qt_parquet_pk_1  """SELECT * FROM sc_parquet_pk order by id;"""
        qt_parquet_pk_2  """SELECT full_name, location FROM sc_parquet_pk order by id;"""
        qt_parquet_pk_3  """SELECT * FROM sc_parquet_pk WHERE salary IS  NULL order by id;"""
        qt_parquet_pk_4  """SELECT * FROM sc_parquet_pk WHERE salary IS NOT NULL order by id;"""
        qt_parquet_pk_5  """SELECT * FROM sc_parquet_pk WHERE location = 'New York' OR location = 'Los Angeles'  order by id;"""
        qt_parquet_pk_6  """SELECT * FROM sc_parquet_pk WHERE id > 5 order by id;"""
        qt_parquet_pk_7  """SELECT * FROM sc_parquet_pk WHERE salary > 6000 order by id;"""


        qt_desc_2 """ desc sc_orc_pk """
        qt_orc_pk_1 """SELECT * FROM sc_orc_pk order by id;"""
        qt_orc_pk_2 """SELECT full_name, location FROM sc_orc_pk order by id;"""
        qt_orc_pk_3 """SELECT * FROM sc_orc_pk WHERE salary IS  NULL order by id;"""
        qt_orc_pk_4 """SELECT * FROM sc_orc_pk WHERE salary IS NOT NULL order by id;"""
        qt_orc_pk_5 """SELECT * FROM sc_orc_pk WHERE location = 'New York' OR location = 'Los Angeles'  order by id;"""
        qt_orc_pk_6 """SELECT * FROM sc_orc_pk WHERE id > 5 order by id;"""
        qt_orc_pk_7 """SELECT * FROM sc_orc_pk WHERE salary > 6000 order by id;"""



        qt_desc_3  """ desc sc_parquet """

        qt_parquet_1 """select * from sc_parquet order by k;"""
        qt_parquet_2 """select * from sc_parquet where k >= 3;"""
        qt_parquet_3 """select * from sc_parquet where k <= 1;"""


        qt_desc_4 """ desc sc_orc """
        qt_orc_1  """select * from sc_orc order by k;"""
        qt_orc_2  """select * from sc_orc where k >= 3;"""
        qt_orc_3  """select * from sc_orc where k <= 1;"""


        qt_count_1 """ select count(*) from sc_parquet_pk;"""
        qt_count_2 """ select count(*) from sc_orc_pk;"""
        qt_count_3 """ select count(*) from sc_parquet;"""
        qt_count_4 """ select count(*) from sc_orc;"""

        // should get latest schema
        qt_desc_latest_schema """ desc test_paimon_spark.test_schema_change; """
        qt_query_latest_schema """ SELECT * FROM test_paimon_spark.test_schema_change; """
        // shoudle get latest schme for branch
        qt_time_travel_schema_branch """ select * from test_paimon_spark.test_schema_change@branch(branch1); """
        // should get the schema in tag
        qt_time_travel_schema_tag """ select * from test_paimon_spark.test_schema_change for version as of 'tag1'; """
        qt_time_travel_schema_tag2 """ select * from test_paimon_spark.test_schema_change@tag(tag1); """
        qt_time_travel_schema_snapshot """ select * from test_paimon_spark.test_schema_change for version as of '1'; """
    }
}


