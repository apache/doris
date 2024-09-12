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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/DataSink.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.nereids.trees.plans.commands.insert.HiveInsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THiveBucket;
import org.apache.doris.thrift.THiveColumn;
import org.apache.doris.thrift.THiveColumnType;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THivePartition;
import org.apache.doris.thrift.THiveSerDeProperties;
import org.apache.doris.thrift.THiveTableSink;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class HiveTableSink extends BaseExternalTableDataSink {
    public static final String PROP_FIELD_DELIMITER = "field.delim";
    public static final String DEFAULT_FIELD_DELIMITER = "\1";
    public static final String PROP_SERIALIZATION_FORMAT = "serialization.format";
    public static final String PROP_LINE_DELIMITER = "line.delim";
    public static final String DEFAULT_LINE_DELIMITER = "\n";
    public static final String PROP_COLLECT_DELIMITER = "collection.delim";
    public static final String DEFAULT_COLLECT_DELIMITER = "\2";
    public static final String PROP_MAPKV_DELIMITER = "mapkv.delim";
    public static final String DEFAULT_MAPKV_DELIMITER = "\3";
    public static final String PROP_ESCAPE_DELIMITER = "escape.delim";
    public static final String DEFAULT_ESCAPE_DELIMIER = "\\";
    public static final String PROP_NULL_FORMAT = "serialization.null.format";
    public static final String DEFAULT_NULL_FORMAT = "\\N";

    private final HMSExternalTable targetTable;
    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_CSV_PLAIN);
            add(TFileFormatType.FORMAT_ORC);
            add(TFileFormatType.FORMAT_PARQUET);
        }};

    public HiveTableSink(HMSExternalTable targetTable) {
        super();
        this.targetTable = targetTable;
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        return supportedTypes;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "HIVE TABLE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return strBuilder.toString();
        }
        // TODO: explain partitions
        return strBuilder.toString();
    }

    @Override
    public void bindDataSink(Optional<InsertCommandContext> insertCtx)
            throws AnalysisException {
        THiveTableSink tSink = new THiveTableSink();
        tSink.setDbName(targetTable.getDbName());
        tSink.setTableName(targetTable.getName());
        Set<String> partNames = new HashSet<>(targetTable.getPartitionColumnNames());
        List<Column> allColumns = targetTable.getColumns();
        Set<String> colNames = allColumns.stream().map(Column::getName).collect(Collectors.toSet());
        colNames.removeAll(partNames);
        List<THiveColumn> targetColumns = new ArrayList<>();
        for (Column col : allColumns) {
            if (partNames.contains(col.getName())) {
                THiveColumn tHiveColumn = new THiveColumn();
                tHiveColumn.setName(col.getName());
                tHiveColumn.setColumnType(THiveColumnType.PARTITION_KEY);
                targetColumns.add(tHiveColumn);
            } else if (colNames.contains(col.getName())) {
                THiveColumn tHiveColumn = new THiveColumn();
                tHiveColumn.setName(col.getName());
                tHiveColumn.setColumnType(THiveColumnType.REGULAR);
                targetColumns.add(tHiveColumn);
            }
        }
        tSink.setColumns(targetColumns);

        setPartitionValues(tSink);

        StorageDescriptor sd = targetTable.getRemoteTable().getSd();
        THiveBucket bucketInfo = new THiveBucket();
        bucketInfo.setBucketedBy(sd.getBucketCols());
        bucketInfo.setBucketCount(sd.getNumBuckets());
        tSink.setBucketInfo(bucketInfo);

        TFileFormatType formatType = getTFileFormatType(sd.getInputFormat());
        tSink.setFileFormat(formatType);
        setCompressType(tSink, formatType);
        setSerDeProperties(tSink);

        THiveLocationParams locationParams = new THiveLocationParams();
        LocationPath locationPath = new LocationPath(sd.getLocation(), targetTable.getHadoopProperties());
        String location = locationPath.getPath().toString();
        String storageLocation = locationPath.toStorageLocation().toString();
        TFileType fileType = locationPath.getTFileTypeForBE();
        if (fileType == TFileType.FILE_S3) {
            locationParams.setWritePath(storageLocation);
            locationParams.setOriginalWritePath(location);
            locationParams.setTargetPath(location);
            if (insertCtx.isPresent()) {
                HiveInsertCommandContext context = (HiveInsertCommandContext) insertCtx.get();
                tSink.setOverwrite(context.isOverwrite());
                context.setWritePath(storageLocation);
                context.setFileType(fileType);
            }
        } else {
            String writeTempPath = createTempPath(location);
            locationParams.setWritePath(writeTempPath);
            locationParams.setOriginalWritePath(writeTempPath);
            locationParams.setTargetPath(location);
            if (insertCtx.isPresent()) {
                HiveInsertCommandContext context = (HiveInsertCommandContext) insertCtx.get();
                tSink.setOverwrite(context.isOverwrite());
                context.setWritePath(writeTempPath);
                context.setFileType(fileType);
            }
        }
        locationParams.setFileType(fileType);
        tSink.setLocation(locationParams);

        tSink.setHadoopConfig(targetTable.getHadoopProperties());

        tDataSink = new TDataSink(getDataSinkType());
        tDataSink.setHiveTableSink(tSink);
    }

    private String createTempPath(String location) {
        String user = ConnectContext.get().getUserIdentity().getUser();
        return LocationPath.getTempWritePath(location, "/tmp/.doris_staging/" + user);
    }

    private void setCompressType(THiveTableSink tSink, TFileFormatType formatType) {
        String compressType;
        switch (formatType) {
            case FORMAT_ORC:
                compressType = targetTable.getRemoteTable().getParameters().get("orc.compress");
                break;
            case FORMAT_PARQUET:
                compressType = targetTable.getRemoteTable().getParameters().get("parquet.compression");
                break;
            case FORMAT_CSV_PLAIN:
                compressType = ConnectContext.get().getSessionVariable().hiveTextCompression();
                break;
            default:
                compressType = "uncompressed";
                break;
        }
        tSink.setCompressionType(getTFileCompressType(compressType));
    }

    private void setPartitionValues(THiveTableSink tSink) throws AnalysisException {
        List<THivePartition> partitions = new ArrayList<>();
        List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions =
                ((HMSExternalCatalog) targetTable.getCatalog())
                        .getClient().listPartitions(targetTable.getDbName(), targetTable.getName());
        for (org.apache.hadoop.hive.metastore.api.Partition partition : hivePartitions) {
            THivePartition hivePartition = new THivePartition();
            StorageDescriptor sd = partition.getSd();
            hivePartition.setFileFormat(getTFileFormatType(sd.getInputFormat()));

            hivePartition.setValues(partition.getValues());
            THiveLocationParams locationParams = new THiveLocationParams();
            String location = sd.getLocation();
            // pass the same of write path and target path to partition
            locationParams.setWritePath(location);
            locationParams.setTargetPath(location);
            locationParams.setFileType(LocationPath.getTFileTypeForBE(location));
            hivePartition.setLocation(locationParams);
            partitions.add(hivePartition);
        }
        tSink.setPartitions(partitions);
    }

    private void setSerDeProperties(THiveTableSink tSink) {
        THiveSerDeProperties serDeProperties = new THiveSerDeProperties();
        // 1. set field delimiter
        Optional<String> fieldDelim = HiveMetaStoreClientHelper.getSerdeProperty(targetTable.getRemoteTable(),
                PROP_FIELD_DELIMITER);
        Optional<String> serFormat = HiveMetaStoreClientHelper.getSerdeProperty(targetTable.getRemoteTable(),
                PROP_SERIALIZATION_FORMAT);
        serDeProperties.setFieldDelim(HiveMetaStoreClientHelper.getByte(HiveMetaStoreClientHelper.firstPresentOrDefault(
                DEFAULT_FIELD_DELIMITER, fieldDelim, serFormat)));
        // 2. set line delimiter
        Optional<String> lineDelim = HiveMetaStoreClientHelper.getSerdeProperty(targetTable.getRemoteTable(),
                PROP_LINE_DELIMITER);
        serDeProperties.setLineDelim(HiveMetaStoreClientHelper.getByte(HiveMetaStoreClientHelper.firstPresentOrDefault(
                DEFAULT_LINE_DELIMITER, lineDelim)));
        // 3. set collection delimiter
        Optional<String> collectDelim = HiveMetaStoreClientHelper.getSerdeProperty(targetTable.getRemoteTable(),
                PROP_COLLECT_DELIMITER);
        serDeProperties
                .setCollectionDelim(HiveMetaStoreClientHelper.getByte(HiveMetaStoreClientHelper.firstPresentOrDefault(
                        DEFAULT_COLLECT_DELIMITER, collectDelim)));
        // 4. set mapkv delimiter
        Optional<String> mapkvDelim = HiveMetaStoreClientHelper.getSerdeProperty(targetTable.getRemoteTable(),
                PROP_MAPKV_DELIMITER);
        serDeProperties.setMapkvDelim(HiveMetaStoreClientHelper.getByte(HiveMetaStoreClientHelper.firstPresentOrDefault(
                DEFAULT_MAPKV_DELIMITER, mapkvDelim)));
        // 5. set escape delimiter
        Optional<String> escapeDelim = HiveMetaStoreClientHelper.getSerdeProperty(targetTable.getRemoteTable(),
                PROP_ESCAPE_DELIMITER);
        serDeProperties
                .setEscapeChar(HiveMetaStoreClientHelper.getByte(HiveMetaStoreClientHelper.firstPresentOrDefault(
                        DEFAULT_ESCAPE_DELIMIER, escapeDelim)));
        // 6. set null format
        Optional<String> nullFormat = HiveMetaStoreClientHelper.getSerdeProperty(targetTable.getRemoteTable(),
                PROP_NULL_FORMAT);
        serDeProperties.setNullFormat(HiveMetaStoreClientHelper.firstPresentOrDefault(
                DEFAULT_NULL_FORMAT, nullFormat));
        tSink.setSerdeProperties(serDeProperties);
    }

    protected TDataSinkType getDataSinkType() {
        return TDataSinkType.HIVE_TABLE_SINK;
    }
}
