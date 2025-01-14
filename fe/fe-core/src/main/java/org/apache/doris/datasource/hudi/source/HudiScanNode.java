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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.FileFormatUtils;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalSchemaCache;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.datasource.hive.source.HiveScanNode;
import org.apache.doris.datasource.hudi.HudiSchemaCacheValue;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.THudiFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class HudiScanNode extends HiveScanNode {

    private static final Logger LOG = LogManager.getLogger(HudiScanNode.class);

    private boolean isCowTable;

    private final AtomicLong noLogsSplitNum = new AtomicLong(0);

    private HoodieTableMetaClient hudiClient;
    private String basePath;
    private String inputFormat;
    private String serdeLib;
    private List<String> columnNames;
    private List<String> columnTypes;

    private boolean partitionInit = false;
    private HoodieTimeline timeline;
    private String queryInstant;

    private final AtomicReference<UserException> batchException = new AtomicReference<>(null);
    private List<HivePartition> prunedPartitions;
    private final Semaphore splittersOnFlight = new Semaphore(NUM_SPLITTERS_ON_FLIGHT);
    private final AtomicInteger numSplitsPerPartition = new AtomicInteger(NUM_SPLITS_PER_PARTITION);

    private boolean incrementalRead = false;
    private TableScanParams scanParams;
    private IncrementalRelation incrementalRelation;
    private HoodieTableFileSystemView fsView;

    /**
     * External file scan node for Query Hudi table
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column
     * priv
     * eg: s3 tvf
     * These scan nodes do not have corresponding catalog/database/table info, so no
     * need to do priv check
     */
    public HudiScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv,
            Optional<TableScanParams> scanParams, Optional<IncrementalRelation> incrementalRelation,
            SessionVariable sv) {
        super(id, desc, "HUDI_SCAN_NODE", StatisticalType.HUDI_SCAN_NODE, needCheckColumnPriv, sv);
        isCowTable = hmsTable.isHoodieCowTable();
        if (LOG.isDebugEnabled()) {
            if (isCowTable) {
                LOG.debug("Hudi table {} can read as cow/read optimize table", hmsTable.getFullQualifiers());
            } else {
                LOG.debug("Hudi table {} is a mor table, and will use JNI to read data in BE",
                        hmsTable.getFullQualifiers());
            }
        }
        this.scanParams = scanParams.orElse(null);
        this.incrementalRelation = incrementalRelation.orElse(null);
        this.incrementalRead = (this.scanParams != null && this.scanParams.incrementalRead());
    }

    @Override
    public TFileFormatType getFileFormatType() throws UserException {
        if (canUseNativeReader()) {
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

        hudiClient = hmsTable.getHudiClient();
        hudiClient.reloadActiveTimeline();
        basePath = hmsTable.getRemoteTable().getSd().getLocation();
        inputFormat = hmsTable.getRemoteTable().getSd().getInputFormat();
        serdeLib = hmsTable.getRemoteTable().getSd().getSerdeInfo().getSerializationLib();
        ExternalSchemaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr().getSchemaCache(table.getCatalog());
        Optional<SchemaCacheValue> schemaCacheValue = cache.getSchemaValue(table.getDbName(), table.getName());
        HudiSchemaCacheValue hudiSchemaCacheValue = (HudiSchemaCacheValue) schemaCacheValue.get();
        columnNames = hudiSchemaCacheValue.getSchema().stream().map(Column::getName).collect(Collectors.toList());
        columnTypes = hudiSchemaCacheValue.getColTypes();

        if (scanParams != null && !scanParams.incrementalRead()) {
            // Only support incremental read
            throw new UserException("Not support function '" + scanParams.getParamType() + "' in hudi table");
        }
        if (incrementalRead) {
            if (isCowTable) {
                try {
                    Map<String, String> serd = hmsTable.getRemoteTable().getSd().getSerdeInfo().getParameters();
                    if ("true".equals(serd.get("hoodie.query.as.ro.table"))
                            && hmsTable.getRemoteTable().getTableName().endsWith("_ro")) {
                        // Incremental read RO table as RT table, I don't know why?
                        isCowTable = false;
                        LOG.warn("Execute incremental read on RO table: {}", hmsTable.getFullQualifiers());
                    }
                } catch (Exception e) {
                    // ignore
                }
            }
            if (incrementalRelation == null) {
                throw new UserException("Failed to create incremental relation");
            }
        }

        timeline = hudiClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
        TableSnapshot tableSnapshot = getQueryTableSnapshot();
        if (tableSnapshot != null) {
            if (tableSnapshot.getType() == TableSnapshot.VersionType.VERSION) {
                throw new UserException("Hudi does not support `FOR VERSION AS OF`, please use `FOR TIME AS OF`");
            }
            queryInstant = tableSnapshot.getTime().replaceAll("[-: ]", "");
        } else {
            Option<HoodieInstant> snapshotInstant = timeline.lastInstant();
            if (!snapshotInstant.isPresent()) {
                prunedPartitions = Collections.emptyList();
                partitionInit = true;
                return;
            }
            queryInstant = snapshotInstant.get().getTimestamp();
        }
        fsView = Env.getCurrentEnv()
            .getExtMetaCacheMgr()
            .getFsViewProcessor(hmsTable.getCatalog())
            .getFsView(hmsTable.getDbName(), hmsTable.getName(), hudiClient);
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        if (incrementalRead) {
            return incrementalRelation.getHoodieParams();
        } else {
            // HudiJniScanner uses hadoop client to read data.
            return hmsTable.getHadoopProperties();
        }
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof HudiSplit) {
            HudiSplit hudiSplit = (HudiSplit) split;
            if (rangeDesc.getFormatType() == TFileFormatType.FORMAT_JNI
                    && !sessionVariable.isForceJniScanner()
                    && hudiSplit.getHudiDeltaLogs().isEmpty()) {
                // no logs, is read optimize table, fallback to use native reader
                String fileFormat = FileFormatUtils.getFileFormatBySuffix(hudiSplit.getDataFilePath())
                        .orElse("Unknown");
                if (fileFormat.equals("parquet")) {
                    rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
                } else if (fileFormat.equals("orc")) {
                    rangeDesc.setFormatType(TFileFormatType.FORMAT_ORC);
                } else {
                    throw new RuntimeException("Unsupported file format: " + fileFormat);
                }
            }
            setHudiParams(rangeDesc, hudiSplit);
        }
    }

    private void setHudiParams(TFileRangeDesc rangeDesc, HudiSplit hudiSplit) {
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
        fileDesc.setHudiJniScanner(hudiSplit.getHudiJniScanner());
        tableFormatFileDesc.setHudiParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    private boolean canUseNativeReader() {
        return !sessionVariable.isForceJniScanner() && isCowTable;
    }

    private List<HivePartition> getPrunedPartitions(HoodieTableMetaClient metaClient) {
        List<Type> partitionColumnTypes = hmsTable.getPartitionColumnTypes();
        if (!partitionColumnTypes.isEmpty()) {
            this.totalPartitionNum = selectedPartitions.totalPartitionNum;
            Map<String, PartitionItem> prunedPartitions = selectedPartitions.selectedPartitions;
            this.selectedPartitionNum = prunedPartitions.size();

            String dbName = hmsTable.getDbName();
            String tblName = hmsTable.getName();
            String inputFormat = hmsTable.getRemoteTable().getSd().getInputFormat();
            String basePath = metaClient.getBasePathV2().toString();

            List<HivePartition> hivePartitions = Lists.newArrayList();
            prunedPartitions.forEach(
                    (key, value) -> {
                        String path = basePath + "/" + key;
                        hivePartitions.add(new HivePartition(
                                dbName, tblName, false, inputFormat, path,
                                ((ListPartitionItem) value).getItems().get(0).getPartitionValuesAsStringList(),
                                Maps.newHashMap()));
                    }
            );
            return hivePartitions;
        }
        // unpartitioned table, create a dummy partition to save location and
        // inputformat,
        // so that we can unify the interface.
        HivePartition dummyPartition = new HivePartition(hmsTable.getDbName(), hmsTable.getName(), true,
                hmsTable.getRemoteTable().getSd().getInputFormat(),
                hmsTable.getRemoteTable().getSd().getLocation(), null, Maps.newHashMap());
        this.totalPartitionNum = 1;
        this.selectedPartitionNum = 1;
        return Lists.newArrayList(dummyPartition);
    }

    private List<Split> getIncrementalSplits() {
        if (canUseNativeReader()) {
            List<Split> splits = incrementalRelation.collectSplits();
            noLogsSplitNum.addAndGet(splits.size());
            return splits;
        }
        Option<String[]> partitionColumns = hudiClient.getTableConfig().getPartitionFields();
        List<String> partitionNames = partitionColumns.isPresent() ? Arrays.asList(partitionColumns.get())
                : Collections.emptyList();
        return incrementalRelation.collectFileSlices().stream().map(fileSlice -> generateHudiSplit(fileSlice,
                HudiPartitionProcessor.parsePartitionValues(partitionNames, fileSlice.getPartitionPath()),
                incrementalRelation.getEndTs())).collect(Collectors.toList());
    }

    private void getPartitionSplits(HivePartition partition, List<Split> splits) throws IOException {

        String partitionName;
        if (partition.isDummyPartition()) {
            partitionName = "";
        } else {
            partitionName = FSUtils.getRelativePartitionPath(hudiClient.getBasePathV2(),
                    new StoragePath(partition.getPath()));
        }

        if (canUseNativeReader()) {
            fsView.getLatestBaseFilesBeforeOrOn(partitionName, queryInstant).forEach(baseFile -> {
                noLogsSplitNum.incrementAndGet();
                String filePath = baseFile.getPath();
                long fileSize = baseFile.getFileSize();
                // Need add hdfs host to location
                LocationPath locationPath = new LocationPath(filePath, hmsTable.getCatalogProperties());
                splits.add(new FileSplit(locationPath, 0, fileSize, fileSize, 0,
                        new String[0], partition.getPartitionValues()));
            });
        } else {
            fsView.getLatestMergedFileSlicesBeforeOrOn(partitionName, queryInstant)
                    .forEach(fileSlice -> splits.add(
                            generateHudiSplit(fileSlice, partition.getPartitionValues(), queryInstant)));
        }
    }

    private void getPartitionsSplits(List<HivePartition> partitions, List<Split> splits) {
        Executor executor = Env.getCurrentEnv().getExtMetaCacheMgr().getFileListingExecutor();
        CountDownLatch countDownLatch = new CountDownLatch(partitions.size());
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        partitions.forEach(partition -> executor.execute(() -> {
            try {
                getPartitionSplits(partition, splits);
            } catch (Throwable t) {
                throwable.set(t);
            } finally {
                countDownLatch.countDown();
            }
        }));
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        if (throwable.get() != null) {
            throw new RuntimeException(throwable.get().getMessage(), throwable.get());
        }
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        if (incrementalRead && !incrementalRelation.fallbackFullTableScan()) {
            return getIncrementalSplits();
        }
        if (!partitionInit) {
            try {
                prunedPartitions = hmsTable.getCatalog().getPreExecutionAuthenticator().execute(()
                        -> getPrunedPartitions(hudiClient));
            } catch (Exception e) {
                throw new UserException(ExceptionUtils.getRootCauseMessage(e), e);
            }
            partitionInit = true;
        }
        List<Split> splits = Collections.synchronizedList(new ArrayList<>());
        getPartitionsSplits(prunedPartitions, splits);
        return splits;
    }

    @Override
    public void startSplit(int numBackends) {
        if (prunedPartitions.isEmpty()) {
            splitAssignment.finishSchedule();
            return;
        }
        AtomicInteger numFinishedPartitions = new AtomicInteger(0);
        CompletableFuture.runAsync(() -> {
            for (HivePartition partition : prunedPartitions) {
                if (batchException.get() != null || splitAssignment.isStop()) {
                    break;
                }
                try {
                    splittersOnFlight.acquire();
                } catch (InterruptedException e) {
                    batchException.set(new UserException(e.getMessage(), e));
                    break;
                }
                CompletableFuture.runAsync(() -> {
                    try {
                        List<Split> allFiles = Lists.newArrayList();
                        getPartitionSplits(partition, allFiles);
                        if (allFiles.size() > numSplitsPerPartition.get()) {
                            numSplitsPerPartition.set(allFiles.size());
                        }
                        splitAssignment.addToQueue(allFiles);
                    } catch (IOException e) {
                        batchException.set(new UserException(e.getMessage(), e));
                    } finally {
                        splittersOnFlight.release();
                        if (batchException.get() != null) {
                            splitAssignment.setException(batchException.get());
                        }
                        if (numFinishedPartitions.incrementAndGet() == prunedPartitions.size()) {
                            splitAssignment.finishSchedule();
                        }
                    }
                });
            }
            if (batchException.get() != null) {
                splitAssignment.setException(batchException.get());
            }
        });
    }

    @Override
    public boolean isBatchMode() {
        if (incrementalRead && !incrementalRelation.fallbackFullTableScan()) {
            return false;
        }
        if (!partitionInit) {
            // Non partition table will get one dummy partition
            try {
                prunedPartitions = hmsTable.getCatalog().getPreExecutionAuthenticator().execute(()
                        -> getPrunedPartitions(hudiClient));
            } catch (Exception e) {
                throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
            }
            partitionInit = true;
        }
        int numPartitions = sessionVariable.getNumPartitionsInBatchMode();
        return numPartitions >= 0 && prunedPartitions.size() >= numPartitions;
    }

    @Override
    public int numApproximateSplits() {
        return numSplitsPerPartition.get() * prunedPartitions.size();
    }

    private HudiSplit generateHudiSplit(FileSlice fileSlice, List<String> partitionValues, String queryInstant) {
        Optional<HoodieBaseFile> baseFile = fileSlice.getBaseFile().toJavaOptional();
        String filePath = baseFile.map(BaseFile::getPath).orElse("");
        long fileSize = baseFile.map(BaseFile::getFileSize).orElse(0L);
        fileSlice.getPartitionPath();

        List<String> logs = fileSlice.getLogFiles().map(HoodieLogFile::getPath)
                .map(StoragePath::toString)
                .collect(Collectors.toList());
        if (logs.isEmpty() && !sessionVariable.isForceJniScanner()) {
            noLogsSplitNum.incrementAndGet();
        }

        // no base file, use log file to parse file type
        String agencyPath = filePath.isEmpty() ? logs.get(0) : filePath;
        HudiSplit split = new HudiSplit(new LocationPath(agencyPath, hmsTable.getCatalogProperties()),
                0, fileSize, fileSize, new String[0], partitionValues);
        split.setTableFormatType(TableFormatType.HUDI);
        split.setDataFilePath(filePath);
        split.setHudiDeltaLogs(logs);
        split.setInputFormat(inputFormat);
        split.setSerde(serdeLib);
        split.setBasePath(basePath);
        split.setHudiColumnNames(columnNames);
        split.setHudiColumnTypes(columnTypes);
        split.setInstantTime(queryInstant);
        split.setHudiJniScanner(sessionVariable.getHudiJniScanner());
        return split;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        if (isBatchMode()) {
            return super.getNodeExplainString(prefix, detailLevel);
        } else {
            return super.getNodeExplainString(prefix, detailLevel)
                    + String.format("%shudiNativeReadSplits=%d/%d\n", prefix, noLogsSplitNum.get(), selectedSplitNum);
        }
    }
}
