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

suite("test_hive_partition_statistic", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        logger.info("This feature has not been supported yet, skip it.")
        /**
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_hive_partition_statistic"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")

        sql """use ${catalog_name}.multi_partition"""
        sql """analyze table multi_partition_orc partitions (`event_day=2008-09-25`, `event_day=1956-09-07`) with sync"""

        def ctlId
        def result = sql """show catalogs"""

        for (int i = 0; i < result.size(); i++) {
            if (result[i][1] == catalog_name) {
                ctlId = result[i][0]
            }
        }

        qt_01 """select part_id, count(*) from internal.__internal_schema.column_statistics where catalog_id='$ctlId' group by part_id order by part_id;"""
        order_qt_1 """select part_id, count, ndv, null_count, min, max from internal.__internal_schema.column_statistics where catalog_id='$ctlId' and part_id='event_day=2008-09-25'"""
        order_qt_2 """select part_id, count, ndv, null_count, min, max from internal.__internal_schema.column_statistics where catalog_id='$ctlId' and part_id='event_day=1956-09-07'"""

        sql """drop catalog ${catalog_name}""";
        **/
    }
}

