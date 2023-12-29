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

package org.apache.doris.planner.external.hudi;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.HudiUtils;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.external.FileSplit;
import org.apache.doris.planner.external.HiveScanNode;
import org.apache.doris.planner.external.TableFormatType;
import org.apache.doris.planner.external.TablePartitionValues;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.THudiFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class HudiScanNode extends HiveScanNode {

    private static final Logger LOG = LogManager.getLogger(HudiScanNode.class);

    private final boolean isCowOrRoTable;

    private final AtomicLong noLogsSplitNum = new AtomicLong(0);

    /**
     * External file scan node for Query Hudi table
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column priv
     * eg: s3 tvf
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public HudiScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "HUDI_SCAN_NODE", StatisticalType.HUDI_SCAN_NODE, needCheckColumnPriv);
        isCowOrRoTable = hmsTable.isHoodieCowTable() || "skip_merge".equals(
                hmsTable.getCatalogProperties().get("hoodie.datasource.merge.type"));
        if (isCowOrRoTable) {
            LOG.debug("Hudi table {} can read as cow/read optimize table", hmsTable.getName());
        } else {
            LOG.debug("Hudi table {} is a mor table, and will use JNI to read data in BE", hmsTable.getName());
        }
    }

    @Override
    public TFileFormatType getFileFormatType() throws UserException {
        if (isCowOrRoTable) {
            return super.getFileFormatType();
        } else {
            // Use jni to read hudi table in BE
            return TFileFormatType.FORMAT_JNI;
        }
    }

    @Override
    protected void doInitialize() throws UserException {
        ExternalTable table = (ExternalTable) desc.getTable();
        if (table.isView()) {
            throw new AnalysisException(
                    String.format("Querying external view '%s.%s' is not supported", table.getDbName(),
                            table.getName()));
        }
        computeColumnsFilter();
        initBackendPolicy();
        initSchemaParams();
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        if (isCowOrRoTable) {
            return super.getLocationProperties();
        } else {
            // HudiJniScanner uses hadoop client to read data.
            return hmsTable.getHadoopProperties();
        }
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof HudiSplit) {
            setHudiParams(rangeDesc, (HudiSplit) split);
        }
    }

    public void setHudiParams(TFileRangeDesc rangeDesc, HudiSplit hudiSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(hudiSplit.getTableFormatType().value());
        THudiFileDesc fileDesc = new THudiFileDesc();
        fileDesc.setInstantTime(hudiSplit.getInstantTime());
        fileDesc.setSerde(hudiSplit.getSerde());
        fileDesc.setInputFormat(hudiSplit.getInputFormat());
        fileDesc.setBasePath(hudiSplit.getBasePath());
        fileDesc.setDataFilePath(hudiSplit.getDataFilePath());
        fileDesc.setDataFileLength(hudiSplit.getFileLength());
        fileDesc.setDeltaLogs(hudiSplit.getHudiDeltaLogs());
        fileDesc.setColumnNames(hudiSplit.getHudiColumnNames());
        fileDesc.setColumnTypes(hudiSplit.getHudiColumnTypes());
        // TODO(gaoxin): support complex types
        // fileDesc.setNestedFields(hudiSplit.getNestedFields());
        tableFormatFileDesc.setHudiParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    private List<HivePartition> getPrunedPartitions(
            HoodieTableMetaClient metaClient, Option<String> snapshotTimestamp) throws AnalysisException {
        List<Type> partitionColumnTypes = hmsTable.getPartitionColumnTypes();
        if (!partitionColumnTypes.isEmpty()) {
            HudiCachedPartitionProcessor processor = (HudiCachedPartitionProcessor) Env.getCurrentEnv()
                    .getExtMetaCacheMgr().getHudiPartitionProcess(hmsTable.getCatalog());
            TablePartitionValues partitionValues;
            if (snapshotTimestamp.isPresent()) {
                partitionValues = processor.getSnapshotPartitionValues(hmsTable, metaClient, snapshotTimestamp.get());
            } else {
                partitionValues = processor.getPartitionValues(hmsTable, metaClient);
            }
            if (partitionValues != null) {
                // 2. prune partitions by expr
                partitionValues.readLock().lock();
                try {
                    Map<Long, PartitionItem> idToPartitionItem = partitionValues.getIdToPartitionItem();
                    this.totalPartitionNum = idToPartitionItem.size();
                    ListPartitionPrunerV2 pruner = new ListPartitionPrunerV2(idToPartitionItem,
                            hmsTable.getPartitionColumns(), columnNameToRange,
                            partitionValues.getUidToPartitionRange(),
                            partitionValues.getRangeToId(),
                            partitionValues.getSingleColumnRangeMap(),
                            true);
                    Collection<Long> filteredPartitionIds = pruner.prune();
                    this.readPartitionNum = filteredPartitionIds.size();
                    // 3. get partitions from cache
                    String dbName = hmsTable.getDbName();
                    String tblName = hmsTable.getName();
                    String inputFormat = hmsTable.getRemoteTable().getSd().getInputFormat();
                    String basePath = metaClient.getBasePathV2().toString();
                    Map<Long, String> partitionIdToNameMap = partitionValues.getPartitionIdToNameMap();
                    Map<Long, List<String>> partitionValuesMap = partitionValues.getPartitionValuesMap();
                    return filteredPartitionIds.stream().map(id -> {
                        String path = basePath + "/" + partitionIdToNameMap.get(id);
                        return new HivePartition(
                                dbName, tblName, false, inputFormat, path, partitionValuesMap.get(id));
                    }).collect(Collectors.toList());
                } finally {
                    partitionValues.readLock().unlock();
                }
            }
        }
        // unpartitioned table, create a dummy partition to save location and inputformat,
        // so that we can unify the interface.
        HivePartition dummyPartition = new HivePartition(hmsTable.getDbName(), hmsTable.getName(), true,
                hmsTable.getRemoteTable().getSd().getInputFormat(),
                hmsTable.getRemoteTable().getSd().getLocation(), null);
        this.totalPartitionNum = 1;
        this.readPartitionNum = 1;
        return Lists.newArrayList(dummyPartition);
    }

    @Override
    public List<Split> getSplits() throws UserException {
        HoodieTableMetaClient hudiClient = HiveMetaStoreClientHelper.getHudiClient(hmsTable);
        hudiClient.reloadActiveTimeline();
        String basePath = hmsTable.getRemoteTable().getSd().getLocation();
        String inputFormat = hmsTable.getRemoteTable().getSd().getInputFormat();
        String serdeLib = hmsTable.getRemoteTable().getSd().getSerdeInfo().getSerializationLib();

        TableSchemaResolver schemaUtil = new TableSchemaResolver(hudiClient);
        Schema hudiSchema;
        try {
            hudiSchema = HoodieAvroUtils.createHoodieWriteSchema(schemaUtil.getTableAvroSchema());
        } catch (Exception e) {
            throw new RuntimeException("Cannot get hudi table schema.");
        }

        List<String> columnNames = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        List<FieldSchema> allFields = Lists.newArrayList();
        allFields.addAll(hmsTable.getRemoteTable().getSd().getCols());
        allFields.addAll(hmsTable.getRemoteTable().getPartitionKeys());

        for (Schema.Field hudiField : hudiSchema.getFields()) {
            String columnName = hudiField.name().toLowerCase(Locale.ROOT);
            // keep hive metastore column in hudi avro schema.
            Optional<FieldSchema> field = allFields.stream().filter(f -> f.getName().equals(columnName)).findFirst();
            if (!field.isPresent()) {
                String errorMsg = String.format("Hudi column %s not exists in hive metastore.", hudiField.name());
                throw new IllegalArgumentException(errorMsg);
            }
            columnNames.add(columnName);
            String columnType = HudiUtils.fromAvroHudiTypeToHiveTypeString(hudiField.schema());
            columnTypes.add(columnType);
        }

        HoodieTimeline timeline = hudiClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
        String queryInstant;
        Option<String> snapshotTimestamp;
        if (desc.getRef().getTableSnapshot() != null) {
            queryInstant = desc.getRef().getTableSnapshot().getTime();
            snapshotTimestamp = Option.of(queryInstant);
        } else {
            Option<HoodieInstant> snapshotInstant = timeline.lastInstant();
            if (!snapshotInstant.isPresent()) {
                return Collections.emptyList();
            }
            queryInstant = snapshotInstant.get().getTimestamp();
            snapshotTimestamp = Option.empty();
        }
        // Non partition table will get one dummy partition
        List<HivePartition> partitions = HiveMetaStoreClientHelper.ugiDoAs(
                HiveMetaStoreClientHelper.getConfiguration(hmsTable),
                () -> getPrunedPartitions(hudiClient, snapshotTimestamp));
        Executor executor = ((HudiCachedPartitionProcessor) Env.getCurrentEnv()
                .getExtMetaCacheMgr().getHudiPartitionProcess(hmsTable.getCatalog())).getExecutor();
        List<Split> splits = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch countDownLatch = new CountDownLatch(partitions.size());
        partitions.forEach(partition -> executor.execute(() -> {
            String globPath;
            String partitionName = "";
            if (partition.isDummyPartition()) {
                globPath = hudiClient.getBasePathV2().toString() + "/*";
            } else {
                partitionName = FSUtils.getRelativePartitionPath(hudiClient.getBasePathV2(),
                        new Path(partition.getPath()));
                globPath = String.format("%s/%s/*", hudiClient.getBasePathV2().toString(), partitionName);
            }
            List<FileStatus> statuses;
            try {
                statuses = FSUtils.getGlobStatusExcludingMetaFolder(hudiClient.getRawFs(),
                        new Path(globPath));
            } catch (IOException e) {
                throw new RuntimeException("Failed to get hudi file statuses on path: " + globPath, e);
            }
            HoodieTableFileSystemView fileSystemView = new HoodieTableFileSystemView(hudiClient,
                    timeline, statuses.toArray(new FileStatus[0]));

            if (isCowOrRoTable) {
                fileSystemView.getLatestBaseFilesBeforeOrOn(partitionName, queryInstant).forEach(baseFile -> {
                    noLogsSplitNum.incrementAndGet();
                    String filePath = baseFile.getPath();
                    long fileSize = baseFile.getFileSize();
                    // Need add hdfs host to location
                    LocationPath locationPath = new LocationPath(filePath, hmsTable.getCatalogProperties());
                    Path splitFilePath = locationPath.toScanRangeLocation();
                    splits.add(new FileSplit(splitFilePath, 0, fileSize, fileSize,
                            new String[0], partition.getPartitionValues()));
                });
            } else {
                fileSystemView.getLatestMergedFileSlicesBeforeOrOn(partitionName, queryInstant).forEach(fileSlice -> {
                    Optional<HoodieBaseFile> baseFile = fileSlice.getBaseFile().toJavaOptional();
                    String filePath = baseFile.map(BaseFile::getPath).orElse("");
                    long fileSize = baseFile.map(BaseFile::getFileSize).orElse(0L);

                    List<String> logs = fileSlice.getLogFiles().map(HoodieLogFile::getPath)
                            .map(Path::toString)
                            .collect(Collectors.toList());
                    if (logs.isEmpty()) {
                        noLogsSplitNum.incrementAndGet();
                    }

                    // no base file, use log file to parse file type
                    String agencyPath = filePath.isEmpty() ? logs.get(0) : filePath;
                    HudiSplit split = new HudiSplit(new Path(agencyPath), 0, fileSize, fileSize,
                            new String[0], partition.getPartitionValues());
                    split.setTableFormatType(TableFormatType.HUDI);
                    split.setDataFilePath(filePath);
                    split.setHudiDeltaLogs(logs);
                    split.setInputFormat(inputFormat);
                    split.setSerde(serdeLib);
                    split.setBasePath(basePath);
                    split.setHudiColumnNames(columnNames);
                    split.setHudiColumnTypes(columnTypes);
                    split.setInstantTime(queryInstant);
                    splits.add(split);
                });
            }
            countDownLatch.countDown();
        }));
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return splits;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return super.getNodeExplainString(prefix, detailLevel)
                + String.format("%shudiNativeReadSplits=%d/%d\n", prefix, noLogsSplitNum.get(), inputSplitsNum);
    }
}
