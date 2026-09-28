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
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.credentials.VendedCredentialsFactory;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.iceberg.IcebergWriteSchemaContext;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergInsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TIcebergMergeSink;
import org.apache.doris.thrift.TIcebergRewritableDeleteFileSet;
import org.apache.doris.thrift.TSortField;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import org.apache.iceberg.types.Types;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Planner sink for Iceberg UPDATE merge operations.
 * Generates TIcebergMergeSink for BE to write delete files and data files.
 */
public class IcebergMergeSink extends BaseExternalTableDataSink {

    private final IcebergExternalTable targetTable;
    private final Table targetIcebergTable;
    private final DeleteCommandContext deleteContext;
    private final boolean writesDataFiles;
    private final Optional<IcebergWriteSchemaContext> writeSchemaContext;
    private final boolean requireMergeCardinalityCheck;
    private List<TIcebergRewritableDeleteFileSet> rewritableDeleteFileSets = Collections.emptyList();

    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_PARQUET);
            add(TFileFormatType.FORMAT_ORC);
        }};

    // Store PropertiesMap, including vended credentials or static credentials
    private Map<StorageProperties.Type, StorageProperties> storagePropertiesMap;

    public IcebergMergeSink(IcebergExternalTable targetTable, DeleteCommandContext deleteContext) {
        this(targetTable, targetTable.getIcebergTable(), deleteContext,
                true, false, Optional.empty());
    }

    public IcebergMergeSink(IcebergExternalTable targetTable, DeleteCommandContext deleteContext,
                            boolean requireMergeCardinalityCheck) {
        this(targetTable, targetTable.getIcebergTable(), deleteContext, true,
                requireMergeCardinalityCheck, Optional.empty());
    }

    public IcebergMergeSink(IcebergExternalTable targetTable, DeleteCommandContext deleteContext,
                            boolean writesDataFiles, boolean requireMergeCardinalityCheck) {
        this(targetTable, targetTable.getIcebergTable(), deleteContext, writesDataFiles,
                requireMergeCardinalityCheck, Optional.empty());
    }

    public IcebergMergeSink(IcebergExternalTable targetTable, Table targetIcebergTable,
            DeleteCommandContext deleteContext, boolean requireMergeCardinalityCheck) {
        this(targetTable, targetIcebergTable, deleteContext,
                true, requireMergeCardinalityCheck, Optional.empty());
    }

    public IcebergMergeSink(IcebergExternalTable targetTable, Table targetIcebergTable,
            DeleteCommandContext deleteContext, boolean writesDataFiles,
            boolean requireMergeCardinalityCheck) {
        this(targetTable, targetIcebergTable, deleteContext,
                writesDataFiles, requireMergeCardinalityCheck, Optional.empty());
    }

    public IcebergMergeSink(IcebergExternalTable targetTable, DeleteCommandContext deleteContext,
            Optional<IcebergWriteSchemaContext> writeSchemaContext) {
        this(targetTable, targetTable.getIcebergTable(), deleteContext,
                true, false, writeSchemaContext);
    }

    /** Constructor with the schema pinned by the Nereids merge plan. */
    public IcebergMergeSink(IcebergExternalTable targetTable, DeleteCommandContext deleteContext,
            boolean requireMergeCardinalityCheck,
            Optional<IcebergWriteSchemaContext> writeSchemaContext) {
        this(targetTable, targetTable.getIcebergTable(), deleteContext,
                true, requireMergeCardinalityCheck, writeSchemaContext);
    }

    /** Constructor with both metadata generations pinned by analysis. */
    public IcebergMergeSink(IcebergExternalTable targetTable, Table targetIcebergTable,
            DeleteCommandContext deleteContext, boolean requireMergeCardinalityCheck,
            Optional<IcebergWriteSchemaContext> writeSchemaContext) {
        this(targetTable, targetIcebergTable, deleteContext,
                true, requireMergeCardinalityCheck, writeSchemaContext);
    }

    /** Constructor with merge behavior and both metadata generations pinned by analysis. */
    public IcebergMergeSink(IcebergExternalTable targetTable, Table targetIcebergTable,
            DeleteCommandContext deleteContext, boolean writesDataFiles,
            boolean requireMergeCardinalityCheck,
            Optional<IcebergWriteSchemaContext> writeSchemaContext) {
        super();
        if (targetTable.isView()) {
            throw new UnsupportedOperationException("UPDATE on iceberg view is not supported");
        }
        this.targetTable = targetTable;
        this.targetIcebergTable = targetIcebergTable;
        this.deleteContext = deleteContext;
        this.writesDataFiles = writesDataFiles;
        this.writeSchemaContext = writeSchemaContext;
        this.requireMergeCardinalityCheck = requireMergeCardinalityCheck;

        IcebergExternalCatalog catalog = (IcebergExternalCatalog) targetTable.getCatalog();
        storagePropertiesMap = VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials(
                catalog.getCatalogProperty().getMetastoreProperties(),
                catalog.getCatalogProperty().getStoragePropertiesMap(),
                targetIcebergTable);
    }

    public void setRewritableDeleteFileSets(List<TIcebergRewritableDeleteFileSet> deleteFileSets) {
        rewritableDeleteFileSets = deleteFileSets != null ? deleteFileSets : Collections.emptyList();
        if (tDataSink != null && tDataSink.isSetIcebergMergeSink()) {
            tDataSink.getIcebergMergeSink().setRewritableDeleteFileSets(rewritableDeleteFileSets);
        }
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        return supportedTypes;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("ICEBERG MERGE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return strBuilder.toString();
        }
        strBuilder.append(prefix).append("  DeleteType: ")
                .append(deleteContext.getDeleteFileType()).append("\n");
        return strBuilder.toString();
    }

    @Override
    public void bindDataSink(Optional<InsertCommandContext> insertCtx)
            throws AnalysisException {

        TIcebergMergeSink tSink = new TIcebergMergeSink();

        // Serialize exactly the schema/spec that the analyzed merge plan and transaction retain.
        Table icebergTable = targetIcebergTable;

        Optional<IcebergWriteSchemaContext> executorWriteSchemaContext = insertCtx
                .filter(IcebergInsertCommandContext.class::isInstance)
                .map(IcebergInsertCommandContext.class::cast)
                .flatMap(IcebergInsertCommandContext::getWriteSchemaContext);
        if (!executorWriteSchemaContext.equals(writeSchemaContext)) {
            throw new AnalysisException("Iceberg write schema context differs between plan and executor");
        }

        tSink.setDbName(targetTable.getDbName());
        tSink.setTbName(targetTable.getName());

        Schema schema = writeSchemaContext
                .map(IcebergWriteSchemaContext::getSchema)
                .orElseGet(icebergTable::schema);
        int formatVersion = writeSchemaContext
                .map(IcebergWriteSchemaContext::getFormatVersion)
                .orElseGet(() -> IcebergUtils.getFormatVersion(icebergTable));
        if (formatVersion >= 3) {
            schema = IcebergUtils.appendRowLineageFieldsForV3(schema);
        }
        tSink.setFormatVersion(formatVersion);
        String writerSchemaJson = writeSchemaContext
                .map(IcebergWriteSchemaContext::getMergeSchemaJson)
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
        // UPDATE and SQL MERGE share this sink, but only SQL MERGE has the one-source-row invariant.
        tSink.setRequireMergeCardinalityCheck(requireMergeCardinalityCheck);
        tSink.setWritesDataFiles(writesDataFiles);

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
            Set<Integer> baseColumnFieldIds = writeSchemaContext
                    .map(IcebergWriteSchemaContext::getSchema)
                    .orElseGet(icebergTable::schema).columns().stream()
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
        tSink.setTableLocation(originalLocation);
        TFileType fileType = locationPath.getTFileTypeForBE();
        tSink.setFileType(fileType);
        if (fileType.equals(TFileType.FILE_BROKER)) {
            tSink.setBrokerAddresses(getBrokerAddresses(targetTable.getCatalog().bindBrokerName()));
        }

        // delete side
        tSink.setDeleteType(deleteContext.toTFileContent());
        tSink.setPartitionSpecIdForDelete(partitionSpec.specId());

        if (formatVersion >= 3 && !rewritableDeleteFileSets.isEmpty()) {
            tSink.setRewritableDeleteFileSets(rewritableDeleteFileSets);
        }
        tDataSink = new TDataSink(TDataSinkType.ICEBERG_MERGE_SINK);
        tDataSink.setIcebergMergeSink(tSink);
    }
}
