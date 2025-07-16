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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.iceberg.IcebergSchemaCacheValue;
import org.apache.doris.datasource.iceberg.IcebergSnapshotCacheValue;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVSnapshotIdSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIf;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class IcebergDlaTable extends HMSDlaTable {

    private boolean isValidRelatedTableCached = false;
    private boolean isValidRelatedTable = false;

    public IcebergDlaTable(HMSExternalTable table) {
        super(table);
    }

    @Override
    public Map<String, PartitionItem> getAndCopyPartitionItems(Optional<MvccSnapshot> snapshot) {
        return Maps.newHashMap(
                IcebergUtils.getOrFetchSnapshotCacheValue(snapshot, hmsTable)
                .getPartitionInfo().getNameToPartitionItem());
    }

    @Override
    public PartitionType getPartitionType(Optional<MvccSnapshot> snapshot) {
        return isValidRelatedTable() ? PartitionType.RANGE : PartitionType.UNPARTITIONED;
    }

    @Override
    public Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) {
        return getPartitionColumns(snapshot).stream().map(Column::getName).collect(Collectors.toSet());
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        IcebergSnapshotCacheValue snapshotValue =
                IcebergUtils.getOrFetchSnapshotCacheValue(snapshot, hmsTable);
        IcebergSchemaCacheValue schemaValue = IcebergUtils.getSchemaCacheValue(
                hmsTable,
                snapshotValue.getSnapshot().getSchemaId());
        return schemaValue.getPartitionColumns();
    }

    @Override
    public MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context,
                                               Optional<MvccSnapshot> snapshot) throws AnalysisException {
        IcebergSnapshotCacheValue snapshotValue =
                IcebergUtils.getOrFetchSnapshotCacheValue(snapshot, hmsTable);
        long latestSnapshotId = snapshotValue.getPartitionInfo().getLatestSnapshotId(partitionName);
        if (latestSnapshotId <= 0) {
            throw new AnalysisException("can not find partition: " + partitionName);
        }
        return new MTMVSnapshotIdSnapshot(latestSnapshotId);
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        hmsTable.makeSureInitialized();
        IcebergSnapshotCacheValue snapshotValue =
                IcebergUtils.getOrFetchSnapshotCacheValue(snapshot, hmsTable);
        return new MTMVSnapshotIdSnapshot(snapshotValue.getSnapshot().getSnapshotId());
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(Optional<MvccSnapshot> snapshot) throws AnalysisException {
        hmsTable.makeSureInitialized();
        IcebergSnapshotCacheValue snapshotValue =
                IcebergUtils.getOrFetchSnapshotCacheValue(snapshot, hmsTable);
        return new MTMVSnapshotIdSnapshot(snapshotValue.getSnapshot().getSnapshotId());
    }

    @Override
    boolean isPartitionColumnAllowNull() {
        return true;
    }

    @Override
    protected boolean isValidRelatedTable() {
        if (isValidRelatedTableCached) {
            return isValidRelatedTable;
        }
        isValidRelatedTable = false;
        Set<String> allFields = Sets.newHashSet();
        Table table = IcebergUtils.getIcebergTable(hmsTable);
        for (PartitionSpec spec : table.specs().values()) {
            if (spec == null) {
                isValidRelatedTableCached = true;
                return false;
            }
            List<PartitionField> fields = spec.fields();
            if (fields.size() != 1) {
                isValidRelatedTableCached = true;
                return false;
            }
            PartitionField partitionField = spec.fields().get(0);
            String transformName = partitionField.transform().toString();
            if (!IcebergUtils.YEAR.equals(transformName)
                    && !IcebergUtils.MONTH.equals(transformName)
                    && !IcebergUtils.DAY.equals(transformName)
                    && !IcebergUtils.HOUR.equals(transformName)) {
                isValidRelatedTableCached = true;
                return false;
            }
            allFields.add(table.schema().findColumnName(partitionField.sourceId()));
        }
        isValidRelatedTableCached = true;
        isValidRelatedTable = allFields.size() == 1;
        return isValidRelatedTable;
    }
}
