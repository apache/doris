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

package org.apache.doris.catalog;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A lightweight wrapper base class for {@link OlapTable}.
 *
 * <p>It delegates table locks and partition-related operations to the wrapped table to keep the
 * metadata view consistent.
 */
public class OlapTableWrapper extends OlapTable {

    protected final OlapTable originTable;

    protected OlapTableWrapper(OlapTable originTable, String wrapperName, List<Column> baseSchema, KeysType keysType) {
        super(originTable.getId(), wrapperName + "_" + originTable.getName(), baseSchema,
                keysType, originTable.getPartitionInfo(), originTable.getDefaultDistributionInfo());
        this.originTable = originTable;
    }

    protected OlapTableWrapper(OlapTable originTable) {
        super(originTable.getId(), originTable.getName(), originTable.getBaseSchema(),
                originTable.getKeysType(), originTable.getPartitionInfo(), originTable.getDefaultDistributionInfo());
        this.originTable = originTable;
    }

    public OlapTable getOriginTable() {
        return originTable;
    }

    @Override
    public long getBaseIndexId() {
        return originTable.getBaseIndexId();
    }

    @Override
    public MaterializedIndexMeta getIndexMetaByIndexId(long indexId) {
        return originTable.getIndexMetaByIndexId(indexId);
    }

    @Override
    public String getIndexNameById(long indexId) {
        return originTable.getIndexNameById(indexId);
    }

    @Override
    public int getIndexSchemaVersion(long indexId) {
        return originTable.getIndexSchemaVersion(indexId);
    }

    @Override
    public List<Column> getSchemaByIndexId(Long indexId) {
        return originTable.getSchemaByIndexId(indexId);
    }

    @Override
    public List<Column> getSchemaByIndexId(Long indexId, boolean full) {
        return originTable.getSchemaByIndexId(indexId, full);
    }

    @Override
    public void readLock() {
        originTable.readLock();
    }

    @Override
    public boolean tryReadLock(long timeout, TimeUnit unit) {
        return originTable.tryReadLock(timeout, unit);
    }

    @Override
    public void readUnlock() {
        originTable.readUnlock();
    }

    @Override
    public PartitionInfo getPartitionInfo() {
        return originTable.getPartitionInfo();
    }

    @Override
    public Partition getPartition(String partitionName, boolean isTempPartition) {
        return originTable.getPartition(partitionName, isTempPartition);
    }

    @Override
    public Partition getPartition(String partitionName) {
        return originTable.getPartition(partitionName);
    }

    @Override
    public Partition getPartition(long partitionId) {
        return originTable.getPartition(partitionId);
    }

    @Override
    public Set<String> getPartitionNames() {
        return originTable.getPartitionNames();
    }

    @Override
    public List<Long> getPartitionIds() {
        return originTable.getPartitionIds();
    }

    @Override
    public Collection<Partition> getPartitions() {
        return originTable.getPartitions();
    }

    @Override
    public List<Long> selectNonEmptyPartitionIds(Collection<Long> partitionIds) {
        return originTable.selectNonEmptyPartitionIds(partitionIds);
    }

    @Override
    public Set<String> getDistributionColumnNames() {
        return originTable.getDistributionColumnNames();
    }
}
