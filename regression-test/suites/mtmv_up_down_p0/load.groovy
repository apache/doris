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

suite("test_upgrade_downgrade_prepare_mtmv","p0,mtmv,restart_fe,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }
    String suiteName = "mtmv_up_down"
    String hivePrefix = "hive2";
    String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "${hivePrefix}_${suiteName}_catalog"
    String mvName = "${hivePrefix}_${suiteName}_mtmv"

    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties (
        "type"="hms",
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
    );"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        create MATERIALIZED VIEW ${mvName}
        REFRESH auto ON MANUAL
        partition by(part1)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES ('replication_num' = '1')
        as select * from ${catalog_name}.multi_catalog.one_partition a inner join ${catalog_name}.multi_catalog.region b on a.id=b.r_regionkey;
        """
    waitingMTMVTaskFinishedByMvName(mvName)
}
