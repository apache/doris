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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.FetchRemoteTabletSchemaUtil;
import org.apache.doris.common.util.FetchRemoteTabletSchemaUtil.PartitionTablets;
import org.apache.doris.common.util.FetchRemoteTabletSchemaUtil.VersionedTablet;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/*
 * SHOW PROC /dbs/dbId/tableId/index_schema/indexId"
 * show index schema
 */
public class RemoteIndexSchemaProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Field").add("Type").add("Null").add("Key")
            .add("Default").add("Extra")
            .build();

    private final OlapTable table;
    private final long indexId;

    private static final class SchemaGeneration {
        private final long baseIndexId;
        private final int baseSchemaVersion;
        private final int indexSchemaVersion;

        private SchemaGeneration(long baseIndexId, int baseSchemaVersion, int indexSchemaVersion) {
            this.baseIndexId = baseIndexId;
            this.baseSchemaVersion = baseSchemaVersion;
            this.indexSchemaVersion = indexSchemaVersion;
        }

        private boolean matches(SchemaGeneration other) {
            return baseIndexId == other.baseIndexId && baseSchemaVersion == other.baseSchemaVersion
                    && indexSchemaVersion == other.indexSchemaVersion;
        }
    }

    public RemoteIndexSchemaProcDir(OlapTable table, long indexId) {
        this.table = table;
        this.indexId = indexId;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        return snapshot().fetchResult();
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String partitionString) throws AnalysisException {
        List<String> partitionNames = Arrays.asList(partitionString.split(","));
        if (partitionNames.isEmpty()) {
            throw new AnalysisException("Describe table[" + table.getName() + "] failed");
        }
        return snapshot(partitionNames);
    }

    public RemoteIndexSchemaProcNode snapshot() throws AnalysisException {
        return snapshot(ImmutableList.of());
    }

    private RemoteIndexSchemaProcNode snapshot(List<String> partitionNames) throws AnalysisException {
        table.readLock();
        try {
            SchemaGeneration generation = getSchemaGeneration();
            Collection<Partition> partitions = partitionNames.isEmpty()
                    ? table.getPartitions() : getNamedPartitions(partitionNames);
            List<Column> schema = table.getSchemaByIndexId(indexId);
            Set<String> bfColumns = table.getCopiedBfColumns();
            ProcResult baseSchema = IndexSchemaProcNode.createResult(schema, bfColumns, ImmutableList.of());
            List<PartitionTablets> partitionTablets = FetchRemoteTabletSchemaUtil.snapshotPartitions(partitions);
            return new RemoteIndexSchemaProcNode(
                    baseSchema, bfColumns, () -> fetchRemoteSchema(partitionTablets, generation));
        } finally {
            table.readUnlock();
        }
    }

    private List<Column> fetchRemoteSchema(List<PartitionTablets> partitions, SchemaGeneration generation)
            throws AnalysisException {
        if (partitions.isEmpty()) {
            return ImmutableList.of();
        }
        // Cloud partition visibility may require a Meta Service RPC.
        List<VersionedTablet> tablets = FetchRemoteTabletSchemaUtil.collectTablets(partitions);
        FetchRemoteTabletSchemaUtil fetcher;
        table.readLock();
        try {
            checkSchemaGeneration(generation);
            if (tablets.isEmpty()) {
                return ImmutableList.of();
            }
            fetcher = new FetchRemoteTabletSchemaUtil(tablets);
        } finally {
            table.readUnlock();
        }
        List<Column> columns = fetcher.fetch();
        table.readLock();
        try {
            // A light schema change can update the index in place while the workers read rowsets.
            checkSchemaGeneration(generation);
            return columns;
        } finally {
            table.readUnlock();
        }
    }

    private SchemaGeneration getSchemaGeneration() throws AnalysisException {
        MaterializedIndexMeta index = table.getIndexMetaByIndexId(indexId);
        long baseIndexId = table.getBaseIndexId();
        MaterializedIndexMeta baseIndex = table.getIndexMetaByIndexId(baseIndexId);
        if (index == null || baseIndex == null) {
            throw new AnalysisException("Index " + indexId + " does not exist");
        }
        return new SchemaGeneration(baseIndexId, baseIndex.getSchemaVersion(), index.getSchemaVersion());
    }

    private void checkSchemaGeneration(SchemaGeneration generation) throws AnalysisException {
        if (!generation.matches(getSchemaGeneration())) {
            throw new AnalysisException("Table schema changed during extended DESCRIBE. Retry the statement.");
        }
    }

    private List<Partition> getNamedPartitions(List<String> partitionNames) throws AnalysisException {
        List<Partition> partitions = new ArrayList<>(partitionNames.size());
        Set<Long> selected = new HashSet<>();
        for (String name : partitionNames) {
            Partition partition = table.getPartition(name);
            if (partition == null) {
                throw new AnalysisException("Describe table[" + table.getName() + "] failed");
            }
            if (selected.add(partition.getId())) {
                partitions.add(partition);
            }
        }
        return partitions;
    }
}
