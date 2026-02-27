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
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.mvcc.MvccSnapshot;

import com.google.common.collect.Sets;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;

import java.util.List;
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
    public PartitionType getPartitionType(Optional<MvccSnapshot> snapshot) {
        return isValidRelatedTable() ? PartitionType.RANGE : PartitionType.UNPARTITIONED;
    }

    @Override
    public Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) {
        return getPartitionColumns(snapshot).stream().map(Column::getName).collect(Collectors.toSet());
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        return IcebergUtils.getIcebergPartitionColumns(snapshot, hmsTable);
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
