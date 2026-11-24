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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.FetchRemoteTabletSchemaUtil;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

    public RemoteIndexSchemaProcDir(OlapTable table, long indexId) {
        this.table = table;
        this.indexId = indexId;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        return snapshot(ImmutableList.of()).fetchResult();
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

    private RemoteIndexSchemaProcNode snapshot(List<String> partitionNames) throws AnalysisException {
        table.readLock();
        try {
            if (table.getIndexMetaByIndexId(indexId) == null) {
                throw new AnalysisException("Index " + indexId + " does not exist");
            }
            Collection<Partition> partitions = partitionNames.isEmpty()
                    ? table.getPartitions() : getNamedPartitions(partitionNames);
            List<Column> schema = table.getSchemaByIndexId(indexId);
            Set<String> bfColumns = table.getCopiedBfColumns();
            ProcResult baseSchema = IndexSchemaProcNode.createResult(schema, bfColumns, ImmutableList.of());
            return new RemoteIndexSchemaProcNode(
                    FetchRemoteTabletSchemaUtil.snapshotPartitions(partitions), baseSchema, bfColumns);
        } finally {
            table.readUnlock();
        }
    }

    private List<Partition> getNamedPartitions(List<String> partitionNames) throws AnalysisException {
        List<Partition> partitions = new ArrayList<>(partitionNames.size());
        for (String name : partitionNames) {
            Partition partition = table.getPartition(name);
            if (partition == null) {
                throw new AnalysisException("Describe table[" + table.getName() + "] failed");
            }
            partitions.add(partition);
        }
        return partitions;
    }
}
