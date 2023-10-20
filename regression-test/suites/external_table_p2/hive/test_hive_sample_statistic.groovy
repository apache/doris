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

suite("test_hive_sample_statistic", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_hive_sample_statistic"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")

        sql """use ${catalog_name}.tpch_1000_parquet"""
        sql """analyze table part with sample percent 10 with sync;"""

        def result = sql """show table stats part"""
        assertTrue(result.size() == 1)
        assertTrue(Long.parseLong(result[0][2]) >= 200000000)
        assertTrue(Long.parseLong(result[0][2]) < 220000000)

        def ctlId
        result = sql """show proc '/catalogs'"""

        for (int i = 0; i < result.size(); i++) {
            if (result[i][1] == catalog_name) {
                ctlId = result[i][0]
            }
        }

        result = sql """select count from internal.__internal_schema.column_statistics where catalog_id='$ctlId' and col_id='p_partkey'"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] >= 200000000)
        assertTrue(result[0][0] < 220000000)

        result = sql """select count from internal.__internal_schema.column_statistics where catalog_id='$ctlId' and col_id='p_name'"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] >= 200000000)
        assertTrue(result[0][0] < 220000000)

        result = sql """select count from internal.__internal_schema.column_statistics where catalog_id='$ctlId' and col_id='p_mfgr'"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] >= 200000000)
        assertTrue(result[0][0] < 220000000)

        result = sql """select count from internal.__internal_schema.column_statistics where catalog_id='$ctlId' and col_id='p_brand'"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] >= 200000000)
        assertTrue(result[0][0] < 220000000)

        result = sql """select count from internal.__internal_schema.column_statistics where catalog_id='$ctlId' and col_id='p_type'"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] >= 200000000)
        assertTrue(result[0][0] < 220000000)

        result = sql """select count from internal.__internal_schema.column_statistics where catalog_id='$ctlId' and col_id='p_size'"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] >= 200000000)
        assertTrue(result[0][0] < 220000000)

        result = sql """select count from internal.__internal_schema.column_statistics where catalog_id='$ctlId' and col_id='p_container'"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] >= 200000000)
        assertTrue(result[0][0] < 220000000)

        result = sql """select count from internal.__internal_schema.column_statistics where catalog_id='$ctlId' and col_id='p_retailprice'"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] >= 200000000)
        assertTrue(result[0][0] < 220000000)

        result = sql """select count from internal.__internal_schema.column_statistics where catalog_id='$ctlId' and col_id='p_comment'"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] >= 200000000)
        assertTrue(result[0][0] < 220000000)

        sql """drop catalog ${catalog_name}""";
    }
}

