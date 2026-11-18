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

suite("test_hive_orc_rowid_seek_exception", "p0,external,hive,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test")
        return
    }

    // Use a separate catalog and the preloaded Hive fixture, independent of the TopN suite.
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hive2HmsPort")
    sql """drop catalog if exists test_hive_orc_rowid_seek_exception"""
    sql """create catalog test_hive_orc_rowid_seek_exception properties (
        "type" = "hms",
        "hive.metastore.uris" = "thrift://${externalEnvIp}:${hmsPort}"
    )"""
    sql """use test_hive_orc_rowid_seek_exception.global_lazy_mat_db"""

    def debugPoint = GetDebugPoint()
    def previousSettings = sql """SELECT @@enable_file_scanner_v2, @@enable_sql_cache,
                                       @@topn_lazy_materialization_threshold"""
    def rowIdSeekQuery = """SELECT name, value, score FROM orc_topn_lazy_mat_table
                           ORDER BY id LIMIT 5"""
    try {
        // The deprecated option must not prevent phase two from using the V2 reader.
        sql "SET enable_file_scanner_v2=false"
        sql "SET enable_sql_cache=false"
        sql "SET topn_lazy_materialization_threshold=1024"
        explain {
            sql rowIdSeekQuery
            contains("VMaterializeNode")
        }
        try {
            // Inject after RowIDs exist, rather than during first-phase file initialization.
            debugPoint.enableDebugPointForAllBEs("OrcReader::rowid_seek.inject_error")
            test {
                sql rowIdSeekQuery
                exception "injected ORC RowID seek failure"
            }
        } finally {
            debugPoint.disableDebugPointForAllBEs("OrcReader::rowid_seek.inject_error")
        }
        // The materializing query must still return rows after the asynchronous failure.
        test {
            sql rowIdSeekQuery
            rowNum 5
        }
    } finally {
        sql "SET enable_file_scanner_v2=${previousSettings[0][0]}"
        sql "SET enable_sql_cache=${previousSettings[0][1]}"
        sql "SET topn_lazy_materialization_threshold=${previousSettings[0][2]}"
    }
}
