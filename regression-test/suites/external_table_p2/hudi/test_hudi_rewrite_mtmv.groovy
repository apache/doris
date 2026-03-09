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

suite("test_hudi_rewrite_mtmv", "p2,external,hudi") {
    String enabled = context.config.otherConfigs.get("enableHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
        return
    }
    String suiteName = "test_hudi_rewrite_mtmv"
    String catalogName = "${suiteName}_catalog"
    String mvName = "${suiteName}_mv"
    String dbName = context.config.getDbNameByFile(context.file)

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hudiHmsPort = context.config.otherConfigs.get("hudiHmsPort")
    String hudiMinioPort = context.config.otherConfigs.get("hudiMinioPort")
    String hudiMinioAccessKey = context.config.otherConfigs.get("hudiMinioAccessKey")
    String hudiMinioSecretKey = context.config.otherConfigs.get("hudiMinioSecretKey")

    sql """set materialized_view_rewrite_enable_contain_external_table=true;"""
    String mvSql = "SELECT par,count(*) as num FROM ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_1 group by par;";

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog if not exists ${catalogName} properties (
            'type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hudiHmsPort}',
            's3.endpoint' = 'http://${externalEnvIp}:${hudiMinioPort}',
            's3.access_key' = '${hudiMinioAccessKey}',
            's3.secret_key' = '${hudiMinioSecretKey}',
            's3.region' = 'us-east-1',
            'use_path_style' = 'true'
        );
    """

    sql """analyze table ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_1 with sync"""
    sql '''
alter table test_hudi_rewrite_mtmv_catalog.hudi_mtmv_regression_test.hudi_table_1
modify column age set stats (
  'ndv'='10',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='10',
  'row_count'='10'
);
'''

    sql '''
alter table test_hudi_rewrite_mtmv_catalog.hudi_mtmv_regression_test.hudi_table_1
modify column _hoodie_record_key set stats (
  'ndv'='10',
  'num_nulls'='0',
  'min_value'='20250121171615893_0_0',
  'max_value'='20250121171615893_7_1',
  'row_count'='10'
);
'''

    sql '''
alter table test_hudi_rewrite_mtmv_catalog.hudi_mtmv_regression_test.hudi_table_1
modify column id set stats (
  'ndv'='10',
  'num_nulls'='0',
  'min_value'='1',
  'max_value'='10',
  'row_count'='10'
);
'''

    sql '''
alter table test_hudi_rewrite_mtmv_catalog.hudi_mtmv_regression_test.hudi_table_1
modify column _hoodie_file_name set stats (
  'ndv'='2',
  'num_nulls'='0',
  'min_value'='58eabd3f-1996-4cb6-83e4-56fd11cb4e7d-0_0-30-108_20250121171615893.parquet',
  'max_value'='7f98e9ac-bd11-48fd-ac80-9ca6dc1ddb34-0_1-30-109_20250121171615893.parquet',
  'row_count'='10'
);
'''

    sql '''
alter table test_hudi_rewrite_mtmv_catalog.hudi_mtmv_regression_test.hudi_table_1
modify column _hoodie_partition_path set stats (
  'ndv'='2',
  'num_nulls'='0',
  'min_value'='par=a',
  'max_value'='par=b',
  'row_count'='10'
);
'''

    sql '''
alter table test_hudi_rewrite_mtmv_catalog.hudi_mtmv_regression_test.hudi_table_1
modify column _hoodie_commit_seqno set stats (
  'ndv'='10',
  'num_nulls'='0',
  'min_value'='20250121171615893_0_0',
  'max_value'='20250121171615893_1_4',
  'row_count'='10'
);
'''

    sql '''
alter table test_hudi_rewrite_mtmv_catalog.hudi_mtmv_regression_test.hudi_table_1
modify column _hoodie_commit_time set stats (
  'ndv'='1',
  'num_nulls'='0',
  'min_value'='20250121171615893',
  'max_value'='20250121171615893',
  'row_count'='10'
);
'''

    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`par`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
             ${mvSql}
        """
    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_a"))
    assertTrue(showPartitionsResult.toString().contains("p_b"))

    // refresh one partitions
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} partitions(p_a);
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    sql """analyze table ${mvName} with sync"""
    order_qt_refresh_one_partition "SELECT par, num FROM ${mvName} "

    sql """alter table ${mvName} modify column par set stats ('row_count'='1');"""

    mv_rewrite_success(mvSql, mvName)
    order_qt_refresh_one_partition_rewrite "${mvSql}"

    mv_rewrite_success("${mvSql}", "${mvName}")

    // select p_b should not rewrite
    mv_not_part_in("SELECT par,count(*) as num FROM ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_1 where par='b' group by par;", "${mvName}")

    //refresh auto
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} auto
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    sql """analyze table ${mvName} with sync"""
    sql """alter table ${mvName} modify column par set stats ('row_count'='2');"""
    order_qt_refresh_auto "SELECT par, num FROM ${mvName} "

    mv_rewrite_success(mvSql, mvName)
    order_qt_refresh_all_partition_rewrite "${mvSql}"

    mv_rewrite_success("${mvSql}", "${mvName}")

    sql """drop materialized view if exists ${mvName};"""
    sql """drop catalog if exists ${catalogName}"""
}

