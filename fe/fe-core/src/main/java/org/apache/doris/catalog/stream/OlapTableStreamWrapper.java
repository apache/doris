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

package org.apache.doris.catalog.stream;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Util;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TPrimitiveType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;

// runtime-only class for unified query/insert experience, created when bind relation with OlapTableStream
public class OlapTableStreamWrapper extends OlapTable {
    private OlapTableStream stream;
    private OlapTable baseTable;
    protected Map<Long, Pair<Long, Long>> outputUpdateMap = Maps.newHashMap();

    public OlapTableStreamWrapper(OlapTableStream stream, OlapTable baseTable) {
        super(stream.getId(), stream.getName(), stream.getFullSchema(), baseTable.getKeysType(),
                baseTable.getPartitionInfo(), baseTable.getDefaultDistributionInfo());
        this.stream = stream;
        this.baseTable = baseTable;
        this.getOrCreatTableProperty().setEnableUniqueKeyMergeOnWrite(baseTable.getEnableUniqueKeyMergeOnWrite());
    }

    @Override
    public List<Column> getBaseSchema(boolean full) {
        return baseTable.getBaseSchema(full);
    }

    @Override
    public List<Column> getBaseSchema() {
        return baseTable.getBaseSchema();
    }

    // for display
    public String getIndexNameById(long indexId) {
        // always returns base index name
        return baseTable.getName();
    }

    // for olap table to thrift
    @Override
    public void getColumnDesc(long selectedIndexId, List<TColumn> columnsDesc, List<String> keyColumnNames,
                              List<TPrimitiveType> keyColumnTypes) {
        baseTable.getColumnDesc(selectedIndexId, columnsDesc, keyColumnNames, keyColumnTypes);
    }

    @Override
    public int getIndexSchemaVersion(long indexId) {
        return baseTable.getIndexSchemaVersion(indexId);
    }

    // no need for pre agg on olap table stream
    @Override
    public boolean isDupKeysOrMergeOnWrite() {
        return false;
    }

    @Override
    public long getBaseIndexId() {
        return baseTable.getBaseIndexId();
    }

    @Override
    public MaterializedIndexMeta getIndexMetaByIndexId(long indexId) {
        return baseTable.getIndexMetaByIndexId(indexId);
    }

    @Override
    public List<Column> getSchemaByIndexId(Long indexId) {
        // here is base table indexId, we can ignore it and use olap table stream schema
        return getBaseSchema(Util.showHiddenColumns());
    }

    // override all partition methods, olap table stream inherit all partitions from base table
    @Override
    public Partition getPartition(String partitionName) {
        return baseTable.getPartition(partitionName);
    }

    @Override
    public Partition getPartition(long partitionId) {
        return baseTable.getPartition(partitionId);
    }

    @Override
    public Partition getPartition(String partitionName, boolean isTempPartition) {
        return baseTable.getPartition(partitionName, isTempPartition);
    }

    @Override
    public List<Long> getPartitionIds() {
        return baseTable.getPartitionIds();
    }

    public Pair<Long, Long> getStreamUpdate(Long partitionId) {
        if (!outputUpdateMap.containsKey(partitionId)) {
            outputUpdateMap.put(partitionId, stream.getStreamUpdate(partitionId));
        }
        return stream.getStreamUpdate(partitionId);
    }

    public Long getStreamDbId() {
        return stream.getDatabase().getId();
    }

    public Long getStreamId() {
        return stream.getId();
    }

    @Override
    public boolean hasDeleteSign() {
        return getDeleteSignColumn() != null;
    }

    @Override
    public boolean getEnableUniqueKeyMergeOnWrite() {
        return baseTable.getEnableUniqueKeyMergeOnWrite();
    }

    @Override
    public boolean isMorTable() {
        return baseTable.isMorTable();
    }

    @Override
    public Collection<Partition> getPartitions() {
        return baseTable.getPartitions();
    }

    @Override
    public List<Long> selectNonEmptyPartitionIds(Collection<Long> partitionIds) {
        List<Long> nonEmptyIds = Lists.newArrayListWithCapacity(partitionIds.size());
        for (Long partitionId : partitionIds) {
            if (stream.hasData(getPartition(partitionId))) {
                nonEmptyIds.add(partitionId);
            }
        }
        return nonEmptyIds;
    }
}
