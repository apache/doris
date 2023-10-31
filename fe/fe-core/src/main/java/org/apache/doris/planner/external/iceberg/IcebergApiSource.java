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

package org.apache.doris.planner.external.iceberg;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.IcebergExternalTable;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.planner.ColumnRange;
import org.apache.doris.planner.external.ExternalFileScanNode;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Get metadata from iceberg api (all iceberg table like hive, rest, glue...)
 */
public class IcebergApiSource implements IcebergSource {

    private final IcebergExternalTable icebergExtTable;
    private final Table originTable;

    private final TupleDescriptor desc;

    public IcebergApiSource(IcebergExternalTable table, TupleDescriptor desc,
                            Map<String, ColumnRange> columnNameToRange) {
        this.icebergExtTable = table;
        this.originTable = ((IcebergExternalCatalog) icebergExtTable.getCatalog())
                .getIcebergTable(icebergExtTable.getDbName(), icebergExtTable.getName());
        this.desc = desc;
    }

    @Override
    public TupleDescriptor getDesc() {
        return desc;
    }

    @Override
    public String getFileFormat() {
        return originTable.properties()
            .getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    }

    @Override
    public Table getIcebergTable() throws MetaNotFoundException {
        return originTable;
    }

    @Override
    public TableIf getTargetTable() {
        return icebergExtTable;
    }

    @Override
    public ExternalFileScanNode.ParamCreateContext createContext() throws UserException {
        ExternalFileScanNode.ParamCreateContext context = new ExternalFileScanNode.ParamCreateContext();
        context.params = new TFileScanRangeParams();
        context.destTupleDescriptor = desc;
        context.params.setDestTupleId(desc.getId().asInt());
        context.fileGroup = new BrokerFileGroup(icebergExtTable.getId(), originTable.location(), getFileFormat());

        // Hive table must extract partition value from path and hudi/iceberg table keep
        // partition field in file.
        List<String> partitionKeys =  originTable.spec().fields().stream()
                .map(PartitionField::name).collect(Collectors.toList());
        List<Column> columns = icebergExtTable.getBaseSchema(false);
        context.params.setNumOfColumnsFromFile(columns.size() - partitionKeys.size());
        updateRequiredSlots(context);
        return context;
    }

    @Override
    public void updateRequiredSlots(ExternalFileScanNode.ParamCreateContext context) throws UserException {
        updateRequiredSlots(context, null);
    }

    public void updateRequiredSlots(ExternalFileScanNode.ParamCreateContext context, List<String> partitionKeys) throws UserException {
        context.params.unsetRequiredSlots();
        if (partitionKeys == null) {
            partitionKeys = originTable.spec().fields().stream().map(PartitionField::name).collect(Collectors.toList());
        }
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
            slotInfo.setSlotId(slot.getId().asInt());
            slotInfo.setIsFileSlot(!partitionKeys.contains(slot.getColumn().getName()));
            context.params.addToRequiredSlots(slotInfo);
        }
    }

    @Override
    public TFileAttributes getFileAttributes() throws UserException {
        return new TFileAttributes();
    }

    @Override
    public ExternalCatalog getCatalog() {
        return icebergExtTable.getCatalog();
    }

}
