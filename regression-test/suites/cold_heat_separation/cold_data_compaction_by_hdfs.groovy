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

import org.apache.hadoop.fs.Path
import java.util.function.Supplier

suite("test_cold_data_compaction_by_hdfs") {

    if (!enableHdfs()) {
        logger.info("Skip this case, because HDFS is not available")
        return
    }

    def retryUntilTimeout = { int timeoutSecond, Supplier<Boolean> closure ->
        long start = System.currentTimeMillis()
        while (true) {
            if (closure.get()) {
                return
            } else {
                if (System.currentTimeMillis() - start > timeoutSecond * 1000) {
                    throw new RuntimeException("" +
                            "Operation timeout, maybe you need to check " +
                            "remove_unused_remote_files_interval_sec and " +
                            "cold_data_compaction_interval_sec in be.conf")
                } else {
                    sleep(10_000)
                }
            }
        }
    }

    String suffix = UUID.randomUUID().hashCode().abs().toString()
    String prefix = "${getHdfsDataDir()}/regression/cold_data_compaction"
    multi_sql """
            DROP TABLE IF EXISTS t_recycle_in_hdfs;
            DROP STORAGE POLICY IF EXISTS test_policy_${suffix};
            DROP RESOURCE IF EXISTS 'remote_hdfs_${suffix}';
            CREATE RESOURCE "remote_hdfs_${suffix}"
            PROPERTIES
            (
                "type"="hdfs",
                "fs.defaultFS"="${getHdfsFs()}",
                "hadoop.username"="${getHdfsUser()}",
                "hadoop.password"="${getHdfsPasswd()}",
                "root_path"="${prefix}"
            );
            CREATE STORAGE POLICY test_policy_${suffix}
            PROPERTIES(
                "storage_resource" = "remote_hdfs_${suffix}",
                "cooldown_ttl" = "5"
            );
            CREATE TABLE IF NOT EXISTS t_recycle_in_hdfs
            (
                k1 BIGINT,
                k2 LARGEINT,
                v1 VARCHAR(2048)
            )
            DISTRIBUTED BY HASH (k1) BUCKETS 1
            PROPERTIES(
                "storage_policy" = "test_policy_${suffix}",
                "disable_auto_compaction" = "true",
                "replication_num" = "1"
            );
        """

    // insert 5 RowSets
    multi_sql """
        insert into t_recycle_in_hdfs values(1, 1, 'Tom');
        insert into t_recycle_in_hdfs values(2, 2, 'Jelly');
        insert into t_recycle_in_hdfs values(3, 3, 'Spike');
        insert into t_recycle_in_hdfs values(4, 4, 'Tyke');
        insert into t_recycle_in_hdfs values(5, 5, 'Tuffy');
    """

    // wait until files upload to S3
    retryUntilTimeout(1800, {
        def res = sql_return_maparray "show data from t_recycle_in_hdfs"
        String size = ""
        String remoteSize = ""
        for (final def line in res) {
            if ("t_recycle_in_hdfs".equals(line.TableName)) {
                size = line.Size
                remoteSize = line.RemoteSize
                break
            }
        }
        logger.info("waiting for data to be uploaded to HDFS: t_recycle_in_hdfs's local data size: ${size}, remote data size: ${remoteSize}")
        return size.startsWith("0") && !remoteSize.startsWith("0")
    })

    String tabletId = sql_return_maparray("show tablets from t_recycle_in_hdfs")[0].TabletId
    // check number of remote files
    def filesBeforeCompaction = getHdfs().listStatus(new Path(prefix + "/data/${tabletId}"))

    // 5 RowSets + 1 meta
    assertEquals(6, filesBeforeCompaction.size())

    // trigger cold data compaction
    sql """alter table t_recycle_in_hdfs set ("disable_auto_compaction" = "false")"""

    // wait until compaction finish
    retryUntilTimeout(1800, {
        def filesAfterCompaction = getHdfs().listStatus(new Path(prefix + "/data/${tabletId}"))
        logger.info("t_recycle_in_hdfs's remote file number is ${filesAfterCompaction.size()}")
        // 1 RowSet + 1 meta
        return filesAfterCompaction.size() == 2
    })

    sql "drop table t_recycle_in_hdfs force"
    retryUntilTimeout(1800, {
        def pathExists = getHdfs().exists(new Path(prefix + "/data/${tabletId}"))
        logger.info("after drop t_recycle_in_hdfs, the remote file path ${pathExists ? "exists" : "not exists"}")
        return !pathExists
    })
}
