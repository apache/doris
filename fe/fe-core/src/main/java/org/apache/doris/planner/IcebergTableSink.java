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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.nereids.trees.plans.commands.insert.BaseExternalTableInsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TIcebergTableSink;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types.NestedField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class IcebergTableSink extends BaseExternalTableDataSink {

    private final IcebergExternalTable targetTable;

    private List<Expr> outputExprs;

    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_ORC);
            add(TFileFormatType.FORMAT_PARQUET);
        }};

    public IcebergTableSink(IcebergExternalTable targetTable) {
        super();
        this.targetTable = targetTable;
    }

    public void setOutputExprs(List<Expr> outputExprs) {
        this.outputExprs = outputExprs;
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        return supportedTypes;
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
    public void bindDataSink(Optional<InsertCommandContext> insertCtx)
            throws AnalysisException {

        TIcebergTableSink tSink = new TIcebergTableSink();

        Table icebergTable = targetTable.getIcebergTable();

        tSink.setDbName(targetTable.getDbName());
        tSink.setTbName(targetTable.getName());

        // schema
        tSink.setSchemaJson(SchemaParser.toJson(icebergTable.schema()));

        // partition spec
        if (icebergTable.spec().isPartitioned()) {
            tSink.setPartitionSpecsJson(Maps.transformValues(icebergTable.specs(), PartitionSpecParser::toJson));
            tSink.setPartitionSpecId(icebergTable.spec().specId());
        }

        // sort order
        if (icebergTable.sortOrder().isSorted()) {
            SortOrder sortOrder = icebergTable.sortOrder();
            ArrayList<Expr> orderingExprs = Lists.newArrayList();
            ArrayList<Boolean> isAscOrder = Lists.newArrayList();
            ArrayList<Boolean> isNullsFirst = Lists.newArrayList();
            for (SortField sortField : sortOrder.fields()) {
                if (!sortField.transform().isIdentity()) {
                    continue;
                }
                for (int i = 0; i < icebergTable.schema().columns().size(); ++i) {
                    NestedField column  = icebergTable.schema().columns().get(i);
                    if (column.fieldId() == sortField.sourceId()) {
                        orderingExprs.add(outputExprs.get(i));
                        isAscOrder.add(sortField.direction().equals(SortDirection.ASC));
                        isNullsFirst.add(sortField.nullOrder().equals(NullOrder.NULLS_FIRST));
                        break;
                    }
                }
            }
            SortInfo sortInfo = new SortInfo(orderingExprs, isAscOrder, isNullsFirst);
            tSink.setSortInfo(sortInfo.toThrift());
        }

        // file info
        tSink.setFileFormat(getTFileFormatType(IcebergUtils.getFileFormat(icebergTable)));
        tSink.setCompressionType(getTFileCompressType(IcebergUtils.getFileCompress(icebergTable)));

        // hadoop config
        HashMap<String, String> props = new HashMap<>(icebergTable.properties());
        Map<String, String> catalogProps = targetTable.getCatalog().getProperties();
        props.putAll(catalogProps);
        tSink.setHadoopConfig(props);

        // location
        LocationPath locationPath = new LocationPath(IcebergUtils.dataLocation(icebergTable), catalogProps);
        tSink.setOutputPath(locationPath.toStorageLocation().toString());
        tSink.setOriginalOutputPath(locationPath.toString());
        tSink.setFileType(locationPath.getTFileTypeForBE());

        if (insertCtx.isPresent()) {
            BaseExternalTableInsertCommandContext context = (BaseExternalTableInsertCommandContext) insertCtx.get();
            tSink.setOverwrite(context.isOverwrite());
        }
        tDataSink = new TDataSink(TDataSinkType.ICEBERG_TABLE_SINK);
        tDataSink.setIcebergTableSink(tSink);
    }
}
