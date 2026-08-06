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
import org.apache.doris.datasource.credentials.VendedCredentialsFactory;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.iceberg.IcebergWriteSchemaContext;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergInsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TIcebergTableSink;
import org.apache.doris.thrift.TIcebergWriteType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types.NestedField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class IcebergTableSink extends BaseExternalTableDataSink {

    private List<Expr> outputExprs;
    private final IcebergExternalTable targetTable;
    private final Table icebergTable;
    private final Optional<IcebergWriteSchemaContext> writeSchemaContext;
    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_ORC);
            add(TFileFormatType.FORMAT_PARQUET);
        }};

    // Store PropertiesMap, including vended credentials or static credentials
    // get them in doInitialize() to ensure internal consistency of ScanNode
    private Map<StorageProperties.Type, StorageProperties> storagePropertiesMap;

    public IcebergTableSink(IcebergExternalTable targetTable) {
        this(targetTable, targetTable.getIcebergTable(), Optional.empty());
    }

    /** Constructor with the schema pinned by the Nereids write plan. */
    public IcebergTableSink(IcebergExternalTable targetTable,
            Optional<IcebergWriteSchemaContext> writeSchemaContext) {
        this(targetTable, targetTable.getIcebergTable(), writeSchemaContext);
    }

    public IcebergTableSink(IcebergExternalTable targetTable, Table icebergTable) {
        this(targetTable, icebergTable, Optional.empty());
    }

    /** Constructor with both metadata generations pinned by analysis. */
    public IcebergTableSink(IcebergExternalTable targetTable, Table icebergTable,
            Optional<IcebergWriteSchemaContext> writeSchemaContext) {
        super();
        if (targetTable.isView()) {
            throw new UnsupportedOperationException("Write data to iceberg view is not supported");
        }
        this.targetTable = targetTable;
        // Keep credentials and every writer option on the metadata generation pinned during analysis.
        this.icebergTable = Objects.requireNonNull(icebergTable, "icebergTable is not null");
        this.writeSchemaContext = Objects.requireNonNull(
                writeSchemaContext, "writeSchemaContext should not be null");
        IcebergExternalCatalog catalog = (IcebergExternalCatalog) targetTable.getCatalog();
        storagePropertiesMap = VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials(
                catalog.getCatalogProperty().getMetastoreProperties(),
                catalog.getCatalogProperty().getStoragePropertiesMap(),
                icebergTable);
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        return supportedTypes;
    }

    public void setOutputExprs(List<Expr> outputExprs) {
        this.outputExprs = outputExprs;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("ICEBERG TABLE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return strBuilder.toString();
        }
        strBuilder.append(prefix).append("Table: ").append(icebergTable.name()).append("\n");
        if (icebergTable.sortOrder().isSorted()) {
            strBuilder.append(prefix).append(targetTable.getSortOrderSql(icebergTable)).append("\n");
        }

        // TODO: explain partitions
        return strBuilder.toString();
    }

    @Override
    public void bindDataSink(Optional<InsertCommandContext> insertCtx)
            throws AnalysisException {

        TIcebergTableSink tSink = new TIcebergTableSink();

        tSink.setDbName(targetTable.getDbName());
        tSink.setTbName(targetTable.getName());

        boolean isRewriting = false;
        Optional<IcebergWriteSchemaContext> executorWriteSchemaContext = Optional.empty();
        if (insertCtx.isPresent() && insertCtx.get() instanceof IcebergInsertCommandContext) {
            IcebergInsertCommandContext context = (IcebergInsertCommandContext) insertCtx.get();
            isRewriting = context.isRewriting();
            executorWriteSchemaContext = context.getWriteSchemaContext();
            if (isRewriting) {
                tSink.setWriteType(TIcebergWriteType.REWRITE);
            }
        }
        if (!executorWriteSchemaContext.equals(writeSchemaContext)) {
            throw new AnalysisException("Iceberg write schema context differs between plan and executor");
        }

        Schema schema = writeSchemaContext
                .map(IcebergWriteSchemaContext::getSchema)
                .orElseGet(icebergTable::schema);
        if (isRewriting
                && IcebergUtils.getFormatVersion(icebergTable) >= IcebergUtils.ICEBERG_ROW_LINEAGE_MIN_VERSION) {
            // iceberg v3 format requires additional row lineage fields when rewrite data files.
            schema = IcebergUtils.appendRowLineageFieldsForV3(schema);
        }
        String writerSchemaJson = isRewriting
                ? SchemaParser.toJson(schema)
                : writeSchemaContext.map(IcebergWriteSchemaContext::getSchemaJson)
                        .orElse(SchemaParser.toJson(schema));
        PartitionSpec partitionSpec = writeSchemaContext
                .map(IcebergWriteSchemaContext::getPartitionSpec)
                .orElseGet(icebergTable::spec);
        SortOrder sortOrder = writeSchemaContext
                .map(IcebergWriteSchemaContext::getSortOrder)
                .orElseGet(icebergTable::sortOrder);
        FileFormat fileFormat = writeSchemaContext
                .map(IcebergWriteSchemaContext::getFileFormat)
                .orElseGet(() -> IcebergUtils.getFileFormat(icebergTable));
        MetricsConfig metricsConfig = writeSchemaContext
                .map(IcebergWriteSchemaContext::getMetricsConfig)
                .orElseGet(() -> MetricsConfig.forTable(icebergTable));
        tSink.setSchemaJson(writerSchemaJson);
        tSink.setCollectColumnStats(
                IcebergUtils.shouldCollectColumnStats(schema, metricsConfig, fileFormat));

        // partition spec
        if (partitionSpec.isPartitioned()) {
            Map<Integer, String> partitionSpecsJson = writeSchemaContext
                    .map(context -> Collections.singletonMap(
                            partitionSpec.specId(), context.getPartitionSpecJson()))
                    .orElseGet(() -> Maps.transformValues(
                            icebergTable.specs(), PartitionSpecParser::toJson));
            tSink.setPartitionSpecsJson(partitionSpecsJson);
            tSink.setPartitionSpecId(partitionSpec.specId());
        }

        // sort order
        if (sortOrder.isSorted()) {
            ArrayList<Expr> orderingExprs = Lists.newArrayList();
            ArrayList<Boolean> isAscOrder = Lists.newArrayList();
            ArrayList<Boolean> isNullsFirst = Lists.newArrayList();
            for (SortField sortField : sortOrder.fields()) {
                if (!sortField.transform().isIdentity()) {
                    continue;
                }
                for (int i = 0; i < schema.columns().size(); ++i) {
                    NestedField column  = schema.columns().get(i);
                    if (column.fieldId() == sortField.sourceId()) {
                        orderingExprs.add(outputExprs.get(i));
                        isAscOrder.add(sortField.direction().equals(SortDirection.ASC));
                        isNullsFirst.add(sortField.nullOrder().equals(NullOrder.NULLS_FIRST));
                        break;
                    }
                }
            }
            SortInfo sortInfo = new SortInfo(orderingExprs, isAscOrder, isNullsFirst, null);
            tSink.setSortInfo(sortInfo.toThrift());
        }

        // file info
        tSink.setFileFormat(getTFileFormatType(fileFormat.name()));
        String fileCompression = writeSchemaContext
                .map(IcebergWriteSchemaContext::getFileCompression)
                .orElseGet(() -> IcebergUtils.getFileCompress(icebergTable));
        tSink.setCompressionType(getTFileCompressType(fileCompression));

        // hadoop config
        Map<String, String> props = new HashMap<>();
        for (StorageProperties storageProperties : storagePropertiesMap.values()) {
            props.putAll(storageProperties.getBackendConfigProperties());
        }
        tSink.setHadoopConfig(props);

        // location
        String originalLocation = writeSchemaContext
                .map(IcebergWriteSchemaContext::getDataLocation)
                .orElseGet(() -> IcebergUtils.dataLocation(icebergTable));
        LocationPath locationPath = LocationPath.of(originalLocation, storagePropertiesMap);
        tSink.setOutputPath(locationPath.toStorageLocation().toString());
        tSink.setOriginalOutputPath(originalLocation);
        TFileType fileType = locationPath.getTFileTypeForBE();
        tSink.setFileType(fileType);
        if (fileType.equals(TFileType.FILE_BROKER)) {
            tSink.setBrokerAddresses(getBrokerAddresses(targetTable.getCatalog().bindBrokerName()));
        }

        if (insertCtx.isPresent() && insertCtx.get() instanceof IcebergInsertCommandContext) {
            IcebergInsertCommandContext context = (IcebergInsertCommandContext) insertCtx.get();
            tSink.setOverwrite(context.isOverwrite());

            // Pass static partition values to BE for static partition overwrite
            if (context.isStaticPartitionOverwrite()) {
                Map<String, String> staticPartitionValues = context.getStaticPartitionValues();
                if (staticPartitionValues != null && !staticPartitionValues.isEmpty()) {
                    tSink.setStaticPartitionValues(staticPartitionValues);
                }
            }
        }
        tDataSink = new TDataSink(TDataSinkType.ICEBERG_TABLE_SINK);
        tDataSink.setIcebergTableSink(tSink);
    }
}
