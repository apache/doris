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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.datasource.hive.HiveProperties;
import org.apache.doris.datasource.mvcc.MvccUtil;
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

import com.google.common.base.Strings;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class HiveTableSink extends BaseExternalTableDataSink {
    private final HMSExternalTable targetTable;
    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_CSV_PLAIN);
            add(TFileFormatType.FORMAT_ORC);
            add(TFileFormatType.FORMAT_PARQUET);
            add(TFileFormatType.FORMAT_TEXT);
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
        String originalLocation = sd.getLocation();
        LocationPath locationPath = LocationPath.of(sd.getLocation(), targetTable.getStoragePropertiesMap());
        String location = sd.getLocation();
        TFileType fileType = locationPath.getTFileTypeForBE();
        if (fileType == TFileType.FILE_S3) {
            locationParams.setWritePath(locationPath.getNormalizedLocation());
            locationParams.setOriginalWritePath(originalLocation);
            locationParams.setTargetPath(locationPath.getNormalizedLocation());
            if (insertCtx.isPresent()) {
                HiveInsertCommandContext context = (HiveInsertCommandContext) insertCtx.get();
                tSink.setOverwrite(context.isOverwrite());
                context.setWritePath(locationPath.getNormalizedLocation());
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
        if (fileType.equals(TFileType.FILE_BROKER)) {
            tSink.setBrokerAddresses(getBrokerAddresses(targetTable.getCatalog().bindBrokerName()));
        }

        tSink.setHadoopConfig(targetTable.getBackendStorageProperties());

        tDataSink = new TDataSink(getDataSinkType());
        tDataSink.setHiveTableSink(tSink);
    }

    private String createTempPath(String location) {
        String user = ConnectContext.get().getCurrentUserIdentity().getUser();
        String stagingBaseDir = targetTable.getCatalog().getCatalogProperty()
                .getOrDefault(HMSExternalCatalog.HIVE_STAGING_DIR, HMSExternalCatalog.DEFAULT_STAGING_BASE_DIR);
        String stagingDir = new Path(stagingBaseDir, user).toString();
        return LocationPath.getTempWritePath(location, stagingDir);
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
            case FORMAT_TEXT:
                compressType = targetTable.getRemoteTable().getParameters().get("text.compression");
                if (Strings.isNullOrEmpty(compressType)) {
                    compressType = ConnectContext.get().getSessionVariable().hiveTextCompression();
                }
                break;
            default:
                compressType = "plain";
                break;
        }
        tSink.setCompressionType(getTFileCompressType(compressType));
    }

    private void setPartitionValues(THiveTableSink tSink) throws AnalysisException {
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setSinkGetPartitionsStartTime();
        }

        List<THivePartition> partitions = new ArrayList<>();

        List<HivePartition> hivePartitions = new ArrayList<>();
        if (targetTable.isPartitionedTable()) {
            // Get partitions from cache instead of HMS client (similar to HiveScanNode)
            HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                    .getMetaStoreCache((HMSExternalCatalog) targetTable.getCatalog());
            HiveMetaStoreCache.HivePartitionValues partitionValues =
                    targetTable.getHivePartitionValues(MvccUtil.getSnapshotFromContext(targetTable));
            List<List<String>> partitionValuesList =
                    new ArrayList<>(partitionValues.getPartitionValuesMap().values());
            hivePartitions = cache.getAllPartitionsWithCache(targetTable, partitionValuesList);
        }

        // Convert HivePartition to THivePartition (same logic as before)
        for (HivePartition partition : hivePartitions) {
            THivePartition hivePartition = new THivePartition();
            hivePartition.setFileFormat(getTFileFormatType(partition.getInputFormat()));
            hivePartition.setValues(partition.getPartitionValues());

            THiveLocationParams locationParams = new THiveLocationParams();
            String location = partition.getPath();
            // pass the same of write path and target path to partition
            locationParams.setWritePath(location);
            locationParams.setTargetPath(location);
            locationParams.setFileType(LocationPath.getTFileTypeForBE(location));
            hivePartition.setLocation(locationParams);
            partitions.add(hivePartition);
        }

        tSink.setPartitions(partitions);

        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setSinkGetPartitionsFinishTime();
        }
    }

    private void setSerDeProperties(THiveTableSink tSink) {
        THiveSerDeProperties serDeProperties = new THiveSerDeProperties();
        Table table = targetTable.getRemoteTable();
        String serDeLib = table.getSd().getSerdeInfo().getSerializationLib();
        // 1. set field delimiter
        if (HiveMetaStoreClientHelper.HIVE_MULTI_DELIMIT_SERDE.equals(serDeLib)) {
            serDeProperties.setFieldDelim(HiveProperties.getFieldDelimiter(table, true));
        } else {
            serDeProperties.setFieldDelim(HiveProperties.getFieldDelimiter(table));
        }
        // 2. set line delimiter
        serDeProperties.setLineDelim(HiveProperties.getLineDelimiter(table));
        // 3. set collection delimiter
        serDeProperties.setCollectionDelim(HiveProperties.getCollectionDelimiter(table));
        // 4. set mapkv delimiter
        serDeProperties.setMapkvDelim(HiveProperties.getMapKvDelimiter(table));
        // 5. set escape delimiter
        HiveProperties.getEscapeDelimiter(table).ifPresent(serDeProperties::setEscapeChar);
        // 6. set null format
        serDeProperties.setNullFormat(HiveProperties.getNullFormat(table));
        tSink.setSerdeProperties(serDeProperties);
    }

    protected TDataSinkType getDataSinkType() {
        return TDataSinkType.HIVE_TABLE_SINK;
    }
}
