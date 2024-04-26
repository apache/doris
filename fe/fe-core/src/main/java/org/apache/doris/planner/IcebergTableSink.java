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

package org.apache.doris.planner;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergInsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TIcebergTableSink;
import org.apache.doris.thrift.TSortField;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

public class IcebergTableSink extends DataSink {

    private IcebergExternalTable targetTable;
    protected TDataSink tDataSink;

    public IcebergTableSink(IcebergExternalTable targetTable) {
        super();
        this.targetTable = targetTable;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("ICEBERG TABLE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return strBuilder.toString();
        }
        // TODO: explain partitions
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        return tDataSink;
    }

    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return DataPartition.RANDOM;
    }

    /**
     * check sink params and generate thrift data sink to BE
     * @param insertCtx insert info context
     * @throws AnalysisException if source file format cannot be read
     */
    public void bindDataSink(Optional<InsertCommandContext> insertCtx)
            throws AnalysisException {

        TIcebergTableSink tSink = new TIcebergTableSink();

        Table icebergTable = targetTable.getIcebergTable();

        tSink.setDbName(targetTable.getDbName());
        tSink.setTbName(targetTable.getName());

        tSink.setSchemaJson(SchemaParser.toJson(icebergTable.schema()));

        if (icebergTable.spec().isPartitioned()) {
            tSink.setPartitionSpecsJson(Maps.transformValues(icebergTable.specs(), PartitionSpecParser::toJson));
            tSink.setPartitionSpecId(icebergTable.spec().specId());
        }

        if (icebergTable.sortOrder().isSorted()) {
            SortOrder sortOrder = icebergTable.sortOrder();
            Set<Integer> baseColumnFieldIds = icebergTable.schema().columns().stream()
                    .map(Types.NestedField::fieldId)
                    .collect(ImmutableSet.toImmutableSet());
            ImmutableList.Builder<TSortField> sortFields = ImmutableList.builder();
            for (SortField sortField : sortOrder.fields()) {
                if (!sortField.transform().isIdentity()) {
                    continue;
                }
                if (!baseColumnFieldIds.contains(sortField.sourceId())) {
                    continue;
                }
                TSortField tSortField = new TSortField();
                tSortField.setSourceColumnId(sortField.sourceId());
                tSortField.setAscending(sortField.direction().equals(SortDirection.ASC));
                tSortField.setNullFirst(sortField.nullOrder().equals(NullOrder.NULLS_FIRST));
                sortFields.add(tSortField);
            }
            tSink.setSortFields(sortFields.build());
        }

        tSink.setFileFormat(getTFileType(icebergTable));
        tSink.setOutputPath(icebergTable.location());
        HashMap<String, String> props = new HashMap<>(icebergTable.properties());
        props.putAll(targetTable.getCatalog().getProperties());
        tSink.setHadoopConfig(props);
        if (insertCtx.isPresent()) {
            IcebergInsertCommandContext context = (IcebergInsertCommandContext) insertCtx.get();
            tSink.setOverwrite(context.isOverwrite());
        }
        tDataSink = new TDataSink(TDataSinkType.ICEBERG_TABLE_SINK);
        tDataSink.setIcebergTableSink(tSink);
    }

    private TFileFormatType getTFileType(Table icebergTable) throws AnalysisException {
        String fileFormat = IcebergUtils.getFileFormat(icebergTable);
        switch (fileFormat.toLowerCase()) {
            case "orc":
                return TFileFormatType.FORMAT_ORC;
            case "parquet":
                return TFileFormatType.FORMAT_PARQUET;
            default:
                throw new AnalysisException("Unsupported input format type: " + fileFormat);
        }
    }
}
