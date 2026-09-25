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

import org.apache.doris.regression.action.ProfileAction
import org.apache.doris.regression.suite.ClusterOptions

suite("test_cloud_memtable_sink_upload_unshared", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(3)
    options.beConfigs += ['share_delta_writers=false', 'enable_packed_file=false',
                          'max_segment_num_per_rowset=1000']
    docker(options) {
        sql "DROP TABLE IF EXISTS test_cloud_sink_upload_source"
        sql "DROP TABLE IF EXISTS test_cloud_sink_upload_unshared"
        sql """
            CREATE TABLE test_cloud_sink_upload_source (k BIGINT, v BIGINT)
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 12
            PROPERTIES ("replication_num"="1")
        """
        sql """
            CREATE TABLE test_cloud_sink_upload_unshared (k BIGINT, v BIGINT)
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true")
        """
        sql "SET enable_sql_cache=false"
        sql "SET enable_memtable_on_sink_node=false"
        sql """
            INSERT INTO test_cloud_sink_upload_source
            SELECT number, number * 2 FROM numbers("number"="100000")
        """
        sql "SET enable_memtable_on_sink_node=true"
        sql "SET enable_cloud_memtable_sink_upload=true"
        sql "SET parallel_pipeline_task_num=4"
        sql "SET profile_level=2"
        sql "SET enable_profile=true"
        sql """
            /* cloud_sink_upload_unshared_profile */
            INSERT INTO test_cloud_sink_upload_unshared
            SELECT * FROM test_cloud_sink_upload_source
        """
        def profileString = new ProfileAction(context).getProfileBySql(
                "cloud_sink_upload_unshared_profile", ["CloudMemtableSinkUpload: true"])
        logger.info("unshared sink-upload profile:\n{}", profileString)
        sql "SET enable_profile=false"
        sql "SET enable_file_cache=false"
        order_qt_rows """
            SELECT COUNT(*), SUM(k), SUM(v) FROM test_cloud_sink_upload_unshared
        """
        def tablet = sql_return_maparray("SHOW TABLETS FROM test_cloud_sink_upload_unshared")[0]
        def partition = sql_return_maparray("SHOW PARTITIONS FROM test_cloud_sink_upload_unshared")[0]
        def ms = cluster.getAllMetaservices()[0]
        def backendCount = sql_return_maparray("SHOW BACKENDS").size()
        getSegmentFilesFromMs("${ms.host}:${ms.httpPort}", tablet.TabletId, partition.VisibleVersion) {
            responseCode, body ->
                assertEquals(200, responseCode)
                def rowsetMeta = parseJson(body)
                def segmentIds = rowsetMeta.segment_ids ?: []
                logger.info("Unshared sink-upload rowset layout: {}", [
                    tablet_id: tablet.TabletId, version: partition.VisibleVersion,
                    rowset_id: rowsetMeta.rowset_id_v2, segment_ids: segmentIds,
                    num_segment_rows: rowsetMeta.num_segment_rows,
                    segments_file_size: rowsetMeta.segments_file_size
                ])
                // One shared writer per BE cannot produce more nonempty ranges than BEs.
                def writerRanges = segmentIds.collect { (it as long).intdiv(1000) }.toSet()
                assertTrue(writerRanges.size() > backendCount,
                        "Expected multiple writers on one source BE for tablet ${tablet.TabletId}: ${segmentIds}")
                order_qt_rowset_layout """
                    SELECT ${segmentIds.size() == (rowsetMeta.num_segments as int)},
                        ${segmentIds.toSet().size() == segmentIds.size()},
                        ${segmentIds.any { (it as long) >= 1000 }},
                        ${rowsetMeta.num_rows as long}
                """
        }
        sql """
            INSERT INTO test_cloud_sink_upload_unshared
            SELECT * FROM test_cloud_sink_upload_source WHERE k < 0
        """
        order_qt_empty_input """
            SELECT COUNT(*), SUM(k), SUM(v) FROM test_cloud_sink_upload_unshared
        """
        // Verify that another transaction can coexist with the sparse-id rowset.
        sql """
            INSERT INTO test_cloud_sink_upload_unshared
            SELECT * FROM test_cloud_sink_upload_source WHERE k < 10
        """
        order_qt_second_load """
            SELECT COUNT(*), SUM(k), SUM(v) FROM test_cloud_sink_upload_unshared
        """
    }
}
