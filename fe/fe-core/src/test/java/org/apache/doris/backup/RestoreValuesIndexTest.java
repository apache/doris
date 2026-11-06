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

package org.apache.doris.backup;

import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.backup.BackupJobInfo.BackupPartitionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand.BackupContent;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestoreValuesIndexTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("restore_values");
    }

    @Test
    public void testRestorePartitionFormatAndReplay() throws Exception {
        boolean original = Config.enable_partition_inverted_index_storage_format_rollout;
        Config.enable_partition_inverted_index_storage_format_rollout = true;
        try {
            createTable("CREATE TABLE restore_values.source (id INT, v VARIANT, "
                    + "INDEX root_idx(v) USING INVERTED PROPERTIES('variant_index_scope'='values','parser'='english')) "
                    + "DUPLICATE KEY(id) PARTITION BY RANGE(id)(PARTITION p1 VALUES [('100'),('200'))) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                    + "PROPERTIES('replication_num'='1','inverted_index_storage_format'='SNII')");
            createTable("CREATE TABLE restore_values.target (id INT, v VARIANT, "
                    + "INDEX root_idx(v) USING INVERTED PROPERTIES('variant_index_scope'='values','parser'='english')) "
                    + "DUPLICATE KEY(id) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN('100')) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                    + "PROPERTIES('replication_num'='1','inverted_index_storage_format'='SNII')");
            var db = Env.getCurrentInternalCatalog().getDbOrMetaException("restore_values");
            OlapTable source = (OlapTable) db.getTableOrMetaException("source");
            OlapTable target = (OlapTable) db.getTableOrMetaException("target");
            long sourceId = source.getPartition("p1").getId();
            source.getPartitionInfo().setInvertedIndexFileStorageFormat(sourceId, TInvertedIndexFileStorageFormat.SNII);
            source.setInvertedIndexFileStorageFormat(TInvertedIndexFileStorageFormat.V2);
            target.setInvertedIndexFileStorageFormat(TInvertedIndexFileStorageFormat.V3);
            target.getTableProperty().getProperties().put("partition.inverted_index_storage_format", "SNII");
            // BackupMeta is independent of the catalog and carries the target's original name.
            source = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), OlapTable.class);
            source.setName("target");
            BackupJobInfo info = new BackupJobInfo();
            info.content = BackupContent.METADATA_ONLY;
            BackupOlapTableInfo tableInfo = new BackupOlapTableInfo();
            BackupPartitionInfo partitionInfo = new BackupPartitionInfo();
            partitionInfo.id = sourceId;
            tableInfo.partitions.put("p1", partitionInfo);
            info.backupOlapTableObjects.put("target", tableInfo);
            RestoreJob job = new RestoreJob("restore_values", "2026-11-06", db.getId(), db.getFullName(), info,
                    false, new ReplicaAllocation((short) 1), 100000, -1,
                    false, false, false, false, false, false, false, false, Env.getCurrentEnv(), 1);
            Deencapsulation.setField(job, "backupMeta", new BackupMeta(List.of(source), List.of()));
            long restoredId = Env.getCurrentEnv().getNextId();
            Partition restored = new Partition(restoredId, "p1",
                    new MaterializedIndex(target.getBaseIndexId(), MaterializedIndex.IndexState.NORMAL),
                    new HashDistributionInfo());
            Deencapsulation.setField(job, "restoredPartitions", List.of(Pair.of("target", restored)));
            job.allReplicasCreated();
            Assertions.assertEquals(TInvertedIndexFileStorageFormat.SNII,
                    target.getInvertedIndexFileStorageFormatForPartition(restoredId));
            Assertions.assertEquals(TInvertedIndexFileStorageFormat.V3, target.getInvertedIndexFileStorageFormat());
            OlapTable checkpoint = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(target), OlapTable.class);
            Assertions.assertEquals(TInvertedIndexFileStorageFormat.SNII,
                    checkpoint.getInvertedIndexFileStorageFormatForPartition(restoredId));

            target.dropPartitionAndReserveTablet("p1");
            job.state = RestoreJob.RestoreJobState.DOWNLOAD;
            ByteArrayOutputStream journal = new ByteArrayOutputStream();
            job.write(new DataOutputStream(journal));
            RestoreJob replay = RestoreJob.read(new DataInputStream(new ByteArrayInputStream(journal.toByteArray())));
            replay.setEnv(Env.getCurrentEnv());
            replay.replayRun();
            Assertions.assertEquals(TInvertedIndexFileStorageFormat.SNII,
                    target.getInvertedIndexFileStorageFormatForPartition(restoredId));
        } finally {
            Config.enable_partition_inverted_index_storage_format_rollout = original;
        }
    }

    @Test
    public void testRestoreRejectsOrdinaryIndexWithSameSignature() throws Exception {
        createTable("CREATE TABLE restore_values.incompatible (id INT, v VARIANT, "
                + "INDEX root_idx(v) USING INVERTED PROPERTIES('variant_index_scope'='values','parser'='english')) "
                + "DUPLICATE KEY(id) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN('100'), "
                + "PARTITION p1 VALUES LESS THAN('200')) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','inverted_index_storage_format'='SNII')");
        var db = Env.getCurrentInternalCatalog().getDbOrMetaException("restore_values");
        OlapTable target = (OlapTable) db.getTableOrMetaException("incompatible");
        OlapTable source = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(target), OlapTable.class);
        source.dropPartitionAndReserveTablet("p0");
        target.dropPartitionAndReserveTablet("p1");
        source.getIndexes().get(0).getProperties().remove("variant_index_scope");
        source.setInvertedIndexFileStorageFormat(TInvertedIndexFileStorageFormat.V2);
        source.getPartitionInfo().setInvertedIndexFileStorageFormat(source.getPartition("p1").getId(),
                TInvertedIndexFileStorageFormat.V2);
        Assertions.assertEquals(target.getSignature(BackupHandler.SIGNATURE_VERSION, new ArrayList<>()),
                source.getSignature(BackupHandler.SIGNATURE_VERSION, new ArrayList<>()));
        BackupJobInfo info = new BackupJobInfo();
        info.content = BackupContent.METADATA_ONLY;
        BackupOlapTableInfo tableInfo = new BackupOlapTableInfo();
        BackupPartitionInfo partitionInfo = new BackupPartitionInfo();
        partitionInfo.id = source.getPartition("p1").getId();
        for (MaterializedIndex materialized : source.getPartition("p1")
                .getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
            BackupJobInfo.BackupIndexInfo indexInfo = new BackupJobInfo.BackupIndexInfo();
            indexInfo.id = materialized.getId();
            indexInfo.schemaHash = source.getSchemaHashByIndexId(materialized.getId());
            for (var tablet : materialized.getTablets()) {
                indexInfo.sortedTabletInfoList.add(new BackupJobInfo.BackupTabletInfo(tablet.getId(), List.of()));
            }
            partitionInfo.indexes.put(source.getIndexNameById(materialized.getId()), indexInfo);
        }
        tableInfo.partitions.put("p1", partitionInfo);
        info.backupOlapTableObjects.put("incompatible", tableInfo);
        RestoreJob job = new RestoreJob("incompatible", "2026-11-06", db.getId(), db.getFullName(), info,
                false, new ReplicaAllocation((short) 1), 100000, -1,
                false, false, false, false, false, false, false, false, Env.getCurrentEnv(), 1);
        BackupMeta backup = new BackupMeta(List.of(source), List.of());
        Repository repo = Mockito.mock(Repository.class);
        Mockito.when(repo.getSnapshotMetaFile(Mockito.any(), Mockito.anyList(), Mockito.anyInt()))
                .thenAnswer(inv -> {
                    List<BackupMeta> metas = inv.getArgument(1);
                    metas.add(backup);
                    return Status.OK;
                });
        Deencapsulation.setField(job, "repo", repo);
        Deencapsulation.invoke(job, "checkAndPrepareMeta");
        Assertions.assertFalse(job.getStatus().ok());
        Assertions.assertTrue(job.getStatus().getErrMsg().contains("Incompatible index definitions"),
                job.getStatus().getErrMsg());
        Assertions.assertNull(target.getPartition("p1"));
    }

    @Test
    public void testValuesIndexRestoreCompatibility() {
        boolean original = Config.enable_partition_inverted_index_storage_format_rollout;
        Config.enable_partition_inverted_index_storage_format_rollout = true;
        try {
            OlapTable target = new OlapTable();
            Map<String, String> properties = Map.of("variant_index_scope", "values", "parser", "english",
                    "support_phrase", "false");
            Index index = new Index(1, "root_idx", List.of("v"), IndexType.INVERTED, properties, "target comment");
            target.setIndexes(List.of(index));
            OlapTable source = new OlapTable();
            source.setPartitionInfo(new PartitionInfo(PartitionType.RANGE));
            source.getPartitionInfo().setInvertedIndexFileStorageFormat(7, TInvertedIndexFileStorageFormat.SNII);
            // Index IDs and comments differ legitimately between independent clusters.
            Index remote = new Index(2, "root_idx", List.of("v"), IndexType.INVERTED, properties, "source comment");
            source.setIndexes(List.of(remote));
            BackupOlapTableInfo info = new BackupOlapTableInfo();
            BackupPartitionInfo part = new BackupPartitionInfo();
            part.id = 7;
            info.partitions.put("p1", part);
            RestoreJob job = new RestoreJob();
            Assertions.assertTrue(job.checkValuesIndexCompatibility(target, source, info).ok());
            for (Map<String, String> difference : List.of(Map.of("variant_index_scope", "paths"),
                    Map.of("parser", "unicode"), Map.of("analyzer", "unicode"),
                    Map.of("support_phrase", "true"), Map.of("lower_case", "false"),
                    Map.of("field_pattern", "a.*"))) {
                Map<String, String> changed = new HashMap<>(index.getProperties());
                changed.putAll(difference);
                remote.setProperties(changed);
                Assertions.assertFalse(job.checkValuesIndexCompatibility(target, source, info).ok(),
                        difference.toString());
            }
            remote.setProperties(new HashMap<>(index.getProperties()));
            for (TInvertedIndexFileStorageFormat format : List.of(TInvertedIndexFileStorageFormat.V2,
                    TInvertedIndexFileStorageFormat.V3)) {
                source.getPartitionInfo().setInvertedIndexFileStorageFormat(7, format);
                Assertions.assertTrue(job.checkValuesIndexCompatibility(target, source, info).getErrMsg()
                        .contains("requires SNII"));
            }
            // Keep the historical behavior of restores between ordinary-index tables.
            target.setIndexes(List.of(new Index(3, "root_idx", List.of("v"), IndexType.INVERTED, Map.of(), "")));
            source.setIndexes(List.of(new Index(4, "root_idx", List.of("v"), IndexType.INVERTED,
                    Map.of("parser", "english"), "")));
            Assertions.assertTrue(job.checkValuesIndexCompatibility(target, source, info).ok());
        } finally {
            Config.enable_partition_inverted_index_storage_format_rollout = original;
        }
    }

}
