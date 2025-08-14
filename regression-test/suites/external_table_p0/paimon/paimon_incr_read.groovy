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

suite("test_paimon_incr_read", "p0,external,doris,external_docker,external_docker_doris") {
    logger.info("start paimon test")
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String catalog_name = "test_paimon_incr_read_catalog"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    try {
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
        sql """use test_paimon_incr_read_db"""

        def test_incr_read = { String force ->
            sql """ set force_jni_scanner=${force} """
            order_qt_snapshot_incr3  """select * from paimon_incr@incr('startSnapshotId'=1, 'endSnapshotId'=2)"""
            order_qt_snapshot_incr4  """select * from paimon_incr@incr('startSnapshotId'=1, 'endSnapshotId'=3)"""
            order_qt_snapshot_incr5  """select * from paimon_incr@incr('startSnapshotId'=2, 'endSnapshotId'=3)"""
            order_qt_timestamp_incr1  """select * from paimon_incr@incr('startTimestamp'=0)"""
            order_qt_timestamp_incr2  """select * from paimon_incr@incr('startTimestamp'=0, 'endTimestamp' = 1)"""
            order_qt_timestamp_incr3  """select * from paimon_incr@incr('startTimestamp'=0, 'endtimestamp' = 999999999999999)"""

            order_qt_scan_mode1 """select * from paimon_incr@incr('startSnapshotId'=1, 'endSnapshotId'=2, 'incrementalBetweenScanMode' = 'auto');"""
            order_qt_scan_mode2 """select * from paimon_incr@incr('startSnapshotId'=1, 'endSnapshotId'=2, 'incrementalBetweenScanMode' = 'diff');"""
            order_qt_scan_mode3 """select * from paimon_incr@incr('startSnapshotId'=1, 'endSnapshotId'=2, 'incrementalBetweenScanMode' = 'delta');"""
            order_qt_scan_mode4 """select * from paimon_incr@incr('startSnapshotId'=1, 'endSnapshotId'=2, 'incrementalBetweenScanMode' = 'changelog');"""
            

            // complex query
            qt_cte """with cte1 as (select * from paimon_incr@incr('startTimestamp'=0)) select name, age from cte1 order by age;"""
            qt_join """select * from paimon_incr@incr('startSnapshotId'=1, 'endSnapshotId'=2) t1 join paimon_incr@incr('startTimestamp'=0) t2 on t1.id = t2.id order by id;"""

            test {
                sql """select * from paimon_incr@incr('startTimestamp'=-1);"""
                exception "startTimestamp must be greater than or equal to 0"
            }
            test {
                sql """select * from paimon_incr@incr('startTimestam'=-1)"""
                exception "at least one valid parameter group must be specified"
            }
            test {
                sql """select * from paimon_incr@incr('endTimestamp'=999999999999999)"""
                exception "startTimestamp is required when using timestamp-based incremental read"
            }
            test {
                sql """select * from paimon_incr@incr()"""
                exception "at least one valid parameter group must be specified"
            }
            test {
                sql """select * from paimon_incr@incr('incrementalBetweenScanMode' = 'auto');"""
                exception "startSnapshotId is required when using snapshot-based incremental read"
            }
            test {
                sql """select * from paimon_incr@incr('startSnapshotId'=1, 'endSnapshotId'=2, 'incrementalBetweenScanMode' = 'error');"""
                exception "incrementalBetweenScanMode must be one of"
            }
            test {
                sql """select * from paimon_incr@incr('startSnapshotId'=1, 'endSnapshotId'=1)"""
                exception "startSnapshotId must be less than endSnapshotId"
            }
            test {
                sql """select * from paimon_incr@incr('startSnapshotId'=1)"""
                exception "endSnapshotId is required when using snapshot-based incremental read"
            }
            test {
                sql """select * from paimon_incr@incr('startSnapshotId'=1, 'endSnapshotId'=2) for version as of 1"""
                exception "Can not specify scan params and table snapshot"
            }
            test {
                sql """select * from paimon_incr for version as of 1"""
                exception "Paimon table does not support table snapshot query yet"
            }
        }

        test_incr_read("false")
        test_incr_read("true")
    } finally {
        // sql """drop catalog if exists ${catalog_name}"""
    }
}


