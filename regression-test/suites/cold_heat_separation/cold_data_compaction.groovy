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

import com.amazonaws.services.s3.model.ListObjectsRequest
import java.util.function.Supplier

suite("test_cold_data_compaction", "nonConcurrent") {
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
    String s3Prefix = "regression/cold_data_compaction"
    multi_sql """
            DROP TABLE IF EXISTS t_recycle_in_s3;
            DROP STORAGE POLICY IF EXISTS test_policy_${suffix};
            DROP RESOURCE IF EXISTS 'remote_s3_${suffix}';

            CREATE RESOURCE "remote_s3_${suffix}"
            PROPERTIES
            (
                "type" = "s3",
                "s3.endpoint" = "${getS3Endpoint()}",
                "s3.region" = "${getS3Region()}",
                "s3.bucket" = "${getS3BucketName()}",
                "s3.root.path" = "${s3Prefix}",
                "s3.access_key" = "${getS3AK()}",
                "s3.secret_key" = "${getS3SK()}",
                "s3.connection.maximum" = "50",
                "s3.connection.request.timeout" = "3000",
                "s3.connection.timeout" = "1000"
            );
            CREATE STORAGE POLICY test_policy_${suffix}
            PROPERTIES(
                "storage_resource" = "remote_s3_${suffix}",
                "cooldown_ttl" = "5"
            );
            CREATE TABLE IF NOT EXISTS t_recycle_in_s3
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
        insert into t_recycle_in_s3 values(1, 1, 'Tom');
        insert into t_recycle_in_s3 values(2, 2, 'Jelly');
        insert into t_recycle_in_s3 values(3, 3, 'Spike');
        insert into t_recycle_in_s3 values(4, 4, 'Tyke');
        insert into t_recycle_in_s3 values(5, 5, 'Tuffy');
    """

    // wait until files upload to S3
    retryUntilTimeout(3600, {
        def res = sql_return_maparray "show data from t_recycle_in_s3"
        String size = ""
        String remoteSize = ""
        for (final def line in res) {
            if ("t_recycle_in_s3".equals(line.TableName)) {
                size = line.Size
                remoteSize = line.RemoteSize
                break
            }
        }
        logger.info("waiting for data to be uploaded to S3: t_recycle_in_s3's local data size: ${size}, remote data size: ${remoteSize}")
        return size.startsWith("0") && !remoteSize.startsWith("0")
    })

    String tabletId = sql_return_maparray("show tablets from t_recycle_in_s3")[0].TabletId

    // get be http ip and port for tabletId
    def tablets = sql_return_maparray("show tablets from t_recycle_in_s3")
    def tablet = tablets.find { it.TabletId == tabletId }
    def backendId = tablet.BackendId
    def bes = sql_return_maparray("show backends")
    def injectBe = bes.find { it.BackendId == backendId }
    def (code, out, err) = be_show_tablet_status(injectBe.Host, injectBe.HttpPort, tabletId)
    logger.info("Run show: code=" + code + ", out=" + out + ", err=" + err)

    // check number of remote files
    def filesBeforeCompaction = getS3Client().listObjects(
            new ListObjectsRequest().withBucketName(getS3BucketName()).withPrefix(s3Prefix + "/data/${tabletId}")).getObjectSummaries()

    // logout files
    logger.info("Files in S3 before compaction:")
    filesBeforeCompaction.each { file ->
        logger.info(" - ${file.getKey()} (size: ${file.getSize()})")
    }
    // 5 RowSets + 1 meta
    assertEquals(6, filesBeforeCompaction.size())

    // trigger cold data compaction
    sql """alter table t_recycle_in_s3 set ("disable_auto_compaction" = "false")"""
    def v = get_be_param("disable_auto_compaction").values().toArray()[0].toString()
    if ("true" == v) {
        set_be_param("disable_auto_compaction", "false")
    }

    // wait until compaction finish
    retryUntilTimeout(3600, {
        def filesAfterCompaction = getS3Client().listObjects(
                new ListObjectsRequest().withBucketName(getS3BucketName()).withPrefix(s3Prefix+ "/data/${tabletId}")).getObjectSummaries()
        logger.info("t_recycle_in_s3's remote file number is ${filesAfterCompaction.size()}")
        // 1 RowSet + 1 meta
        return filesAfterCompaction.size() == 2
    })

    if ("true" == v) {
        set_be_param("disable_auto_compaction", "true")
    }

    sql "drop table t_recycle_in_s3 force"
    retryUntilTimeout(3600, {
        def filesAfterDrop = getS3Client().listObjects(
                new ListObjectsRequest().withBucketName(getS3BucketName()).withPrefix(s3Prefix+ "/data/${tabletId}")).getObjectSummaries()
        logger.info("after drop t_recycle_in_s3, remote file number is ${filesAfterDrop.size()}")
        return filesAfterDrop.size() == 0
    })
}
