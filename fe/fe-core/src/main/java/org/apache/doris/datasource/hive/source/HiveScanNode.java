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

package org.apache.doris.datasource.hive.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.hive.AcidInfo;
import org.apache.doris.datasource.hive.AcidInfo.DeleteDeltaInfo;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheValue;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.datasource.hive.HiveProperties;
import org.apache.doris.datasource.hive.HiveTransaction;
import org.apache.doris.datasource.hive.source.HiveSplit.HiveSplitCreator;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.fs.DirectoryLister;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TTransactionalHiveDeleteDeltaDesc;
import org.apache.doris.thrift.TTransactionalHiveDesc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Setter;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class HiveScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(HiveScanNode.class);

    protected final HMSExternalTable hmsTable;
    private HiveTransaction hiveTransaction = null;

    // will only be set in Nereids, for lagency planner, it should be null
    @Setter
    protected SelectedPartitions selectedPartitions = null;

    private DirectoryLister directoryLister;

    private boolean partitionInit = false;
    private final AtomicReference<UserException> batchException = new AtomicReference<>(null);
    private List<HivePartition> prunedPartitions;
    private final Semaphore splittersOnFlight = new Semaphore(NUM_SPLITTERS_ON_FLIGHT);
    private final AtomicInteger numSplitsPerPartition = new AtomicInteger(NUM_SPLITS_PER_PARTITION);

    private boolean skipCheckingAcidVersionFile = false;

    /**
     * * External file scan node for Query Hive table
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column priv
     * eg: s3 tvf
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public HiveScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv, SessionVariable sv,
            DirectoryLister directoryLister) {
        this(id, desc, "HIVE_SCAN_NODE", needCheckColumnPriv, sv, directoryLister);
    }

    public HiveScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
            boolean needCheckColumnPriv, SessionVariable sv,
            DirectoryLister directoryLister) {
        super(id, desc, planNodeName, needCheckColumnPriv, sv);
        hmsTable = (HMSExternalTable) desc.getTable();
        brokerName = hmsTable.getCatalog().bindBrokerName();
        this.directoryLister = directoryLister;
    }

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();

        if (hmsTable.isHiveTransactionalTable()) {
            this.hiveTransaction = new HiveTransaction(DebugUtil.printId(ConnectContext.get().queryId()),
                    ConnectContext.get().getQualifiedUser(), hmsTable, hmsTable.isFullAcidTable());
            Env.getCurrentHiveTransactionMgr().register(hiveTransaction);
            skipCheckingAcidVersionFile = sessionVariable.skipCheckingAcidVersionFile;
        }
    }

    protected List<HivePartition> getPartitions() throws AnalysisException {
        List<HivePartition> resPartitions = Lists.newArrayList();
        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) hmsTable.getCatalog());
        List<Type> partitionColumnTypes = hmsTable.getPartitionColumnTypes(MvccUtil.getSnapshotFromContext(hmsTable));
        if (!partitionColumnTypes.isEmpty()) {
            // partitioned table
            Collection<PartitionItem> partitionItems;
            // partitions has benn pruned by Nereids, in PruneFileScanPartition,
            // so just use the selected partitions.
            this.totalPartitionNum = selectedPartitions.totalPartitionNum;
            partitionItems = selectedPartitions.selectedPartitions.values();
            Preconditions.checkNotNull(partitionItems);
            this.selectedPartitionNum = partitionItems.size();

            // get partitions from cache
            List<List<String>> partitionValuesList = Lists.newArrayListWithCapacity(partitionItems.size());
            for (PartitionItem item : partitionItems) {
                partitionValuesList.add(
                        ((ListPartitionItem) item).getItems().get(0).getPartitionValuesAsStringListForHive());
            }
            resPartitions = cache.getAllPartitionsWithCache(hmsTable, partitionValuesList);
        } else {
            // non partitioned table, create a dummy partition to save location and inputformat,
            // so that we can unify the interface.
            HivePartition dummyPartition = new HivePartition(hmsTable.getOrBuildNameMapping(), true,
                    hmsTable.getRemoteTable().getSd().getInputFormat(),
                    hmsTable.getRemoteTable().getSd().getLocation(), null, Maps.newHashMap());
            this.totalPartitionNum = 1;
            this.selectedPartitionNum = 1;
            resPartitions.add(dummyPartition);
        }
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setGetPartitionsFinishTime();
        }
        return resPartitions;
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        long start = System.currentTimeMillis();
        try {
            if (!partitionInit) {
                prunedPartitions = getPartitions();
                partitionInit = true;
            }
            HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                    .getMetaStoreCache((HMSExternalCatalog) hmsTable.getCatalog());
            String bindBrokerName = hmsTable.getCatalog().bindBrokerName();
            List<Split> allFiles = Lists.newArrayList();
            getFileSplitByPartitions(cache, prunedPartitions, allFiles, bindBrokerName, numBackends, false);
            if (ConnectContext.get().getExecutor() != null) {
                ConnectContext.get().getExecutor().getSummaryProfile().setGetPartitionFilesFinishTime();
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("get #{} files for table: {}.{}, cost: {} ms",
                        allFiles.size(), hmsTable.getDbName(), hmsTable.getName(),
                        (System.currentTimeMillis() - start));
            }
            return allFiles;
        } catch (Throwable t) {
            LOG.warn("get file split failed for table: {}", hmsTable.getName(), t);
            throw new UserException(
                "get file split failed for table: " + hmsTable.getName() + ", err: " + Util.getRootCauseMessage(t),
                t);
        }
    }

    @Override
    public void startSplit(int numBackends) {
        if (prunedPartitions.isEmpty()) {
            splitAssignment.finishSchedule();
            return;
        }
        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) hmsTable.getCatalog());
        Executor scheduleExecutor = Env.getCurrentEnv().getExtMetaCacheMgr().getScheduleExecutor();
        String bindBrokerName = hmsTable.getCatalog().bindBrokerName();
        AtomicInteger numFinishedPartitions = new AtomicInteger(0);
        CompletableFuture.runAsync(() -> {
            for (HivePartition partition : prunedPartitions) {
                if (batchException.get() != null || splitAssignment.isStop()) {
                    break;
                }
                try {
                    splittersOnFlight.acquire();
                    CompletableFuture.runAsync(() -> {
                        try {
                            List<Split> allFiles = Lists.newArrayList();
                            getFileSplitByPartitions(
                                    cache, Collections.singletonList(partition), allFiles, bindBrokerName,
                                    numBackends, true);
                            if (allFiles.size() > numSplitsPerPartition.get()) {
                                numSplitsPerPartition.set(allFiles.size());
                            }
                            if (splitAssignment.needMoreSplit()) {
                                splitAssignment.addToQueue(allFiles);
                            }
                        } catch (Exception e) {
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
                    }, scheduleExecutor);
                } catch (Exception e) {
                    // When submitting a task, an exception will be thrown if the task pool(scheduleExecutor) is full
                    batchException.set(new UserException(e.getMessage(), e));
                    break;
                }
            }
            if (batchException.get() != null) {
                splitAssignment.setException(batchException.get());
            }
        });
    }

    @Override
    public boolean isBatchMode() {
        if (!partitionInit) {
            try {
                prunedPartitions = getPartitions();
            } catch (Exception e) {
                return false;
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

    private void getFileSplitByPartitions(HiveMetaStoreCache cache, List<HivePartition> partitions,
            List<Split> allFiles, String bindBrokerName, int numBackends,
            boolean isBatchMode) throws IOException, UserException {
        List<FileCacheValue> fileCaches;
        if (hiveTransaction != null) {
            try {
                fileCaches = getFileSplitByTransaction(cache, partitions, bindBrokerName);
            } catch (Exception e) {
                // Release shared load (getValidWriteIds acquire Lock).
                // If no exception is throw, the lock will be released when `finalizeQuery()`.
                Env.getCurrentHiveTransactionMgr().deregister(hiveTransaction.getQueryId());
                throw e;
            }
        } else {
            boolean withCache = Config.max_external_file_cache_num > 0;
            fileCaches = cache.getFilesByPartitions(partitions, withCache, partitions.size() > 1,
                    directoryLister, hmsTable);
        }

        long targetFileSplitSize = determineTargetFileSplitSize(fileCaches, isBatchMode);
        if (tableSample != null) {
            List<HiveMetaStoreCache.HiveFileStatus> hiveFileStatuses = selectFiles(fileCaches);
            splitAllFiles(allFiles, hiveFileStatuses, targetFileSplitSize);
            return;
        }

        /**
         * If the push down aggregation operator is COUNT,
         * we don't need to split the file because for parquet/orc format, only metadata is read.
         * If we split the file, we will read metadata of a file multiple times, which is not efficient.
         *
         * - Hive Full Acid Transactional Table may need merge on read, so do not apply this optimization.
         * - If the file format is not parquet/orc, eg, text, we need to split the file to increase the parallelism.
         */
        boolean needSplit = true;
        if (getPushDownAggNoGroupingOp() == TPushAggOp.COUNT
                && !(hmsTable.isHiveTransactionalTable() && hmsTable.isFullAcidTable())) {
            int totalFileNum = 0;
            for (FileCacheValue fileCacheValue : fileCaches) {
                if (fileCacheValue.getFiles() != null) {
                    totalFileNum += fileCacheValue.getFiles().size();
                }
            }
            int parallelNum = sessionVariable.getParallelExecInstanceNum();
            needSplit = FileSplitter.needSplitForCountPushdown(parallelNum, numBackends, totalFileNum);
        }

        for (HiveMetaStoreCache.FileCacheValue fileCacheValue : fileCaches) {
            if (fileCacheValue.getFiles() == null) {
                continue;
            }
            boolean isSplittable = fileCacheValue.isSplittable();

            for (HiveMetaStoreCache.HiveFileStatus status : fileCacheValue.getFiles()) {
                allFiles.addAll(fileSplitter.splitFile(
                        status.getPath(),
                        targetFileSplitSize,
                        status.getBlockLocations(),
                        status.getLength(),
                        status.getModificationTime(),
                        isSplittable && needSplit,
                        fileCacheValue.getPartitionValues(),
                        new HiveSplitCreator(fileCacheValue.getAcidInfo())));
            }
        }
    }

    private long determineTargetFileSplitSize(List<FileCacheValue> fileCaches,
            boolean isBatchMode) {
        if (sessionVariable.getFileSplitSize() > 0) {
            return sessionVariable.getFileSplitSize();
        }
        /** Hive batch split mode will return 0. and <code>FileSplitter</code>
         *  will determine file split size.
         */
        if (isBatchMode) {
            return 0;
        }
        long result = sessionVariable.getMaxInitialSplitSize();
        long totalFileSize = 0;
        boolean exceedInitialThreshold = false;
        for (HiveMetaStoreCache.FileCacheValue fileCacheValue : fileCaches) {
            if (fileCacheValue.getFiles() == null) {
                continue;
            }
            for (HiveMetaStoreCache.HiveFileStatus status : fileCacheValue.getFiles()) {
                totalFileSize += status.getLength();
                if (!exceedInitialThreshold
                        && totalFileSize >= sessionVariable.getMaxSplitSize()
                                * sessionVariable.getMaxInitialSplitNum()) {
                    exceedInitialThreshold = true;
                }
            }
        }
        result = exceedInitialThreshold ? sessionVariable.getMaxSplitSize() : result;
        result = applyMaxFileSplitNumLimit(result, totalFileSize);
        return result;
    }

    private void splitAllFiles(List<Split> allFiles,
                               List<HiveMetaStoreCache.HiveFileStatus> hiveFileStatuses,
            long realFileSplitSize) throws IOException {
        for (HiveMetaStoreCache.HiveFileStatus status : hiveFileStatuses) {
            allFiles.addAll(fileSplitter.splitFile(
                    status.getPath(),
                    realFileSplitSize,
                    status.getBlockLocations(),
                    status.getLength(),
                    status.getModificationTime(),
                    status.isSplittable(),
                    status.getPartitionValues(),
                    new HiveSplitCreator(status.getAcidInfo())));
        }
    }

    private List<HiveMetaStoreCache.HiveFileStatus> selectFiles(List<FileCacheValue> inputCacheValue) {
        List<HiveMetaStoreCache.HiveFileStatus> fileList = Lists.newArrayList();
        long totalSize = 0;
        for (FileCacheValue value : inputCacheValue) {
            for (HiveMetaStoreCache.HiveFileStatus file : value.getFiles()) {
                file.setSplittable(value.isSplittable());
                file.setPartitionValues(value.getPartitionValues());
                file.setAcidInfo(value.getAcidInfo());
                fileList.add(file);
                totalSize += file.getLength();
            }
        }
        long sampleSize = 0;
        if (tableSample.isPercent()) {
            sampleSize = totalSize * tableSample.getSampleValue() / 100;
        } else {
            long estimatedRowSize = 0;
            for (Column column : hmsTable.getFullSchema()) {
                estimatedRowSize += column.getDataType().getSlotSize();
            }
            sampleSize = estimatedRowSize * tableSample.getSampleValue();
        }
        long selectedSize = 0;
        Collections.shuffle(fileList, new Random(tableSample.getSeek()));
        int index = 0;
        for (HiveMetaStoreCache.HiveFileStatus file : fileList) {
            selectedSize += file.getLength();
            index += 1;
            if (selectedSize >= sampleSize) {
                break;
            }
        }
        return fileList.subList(0, index);
    }

    private List<FileCacheValue> getFileSplitByTransaction(HiveMetaStoreCache cache, List<HivePartition> partitions,
                                                           String bindBrokerName) {
        for (HivePartition partition : partitions) {
            if (partition.getPartitionValues() == null || partition.getPartitionValues().isEmpty()) {
                // this is unpartitioned table.
                continue;
            }
            hiveTransaction.addPartition(partition.getPartitionName(hmsTable.getPartitionColumns()));
        }
        Map<String, String> txnValidIds = hiveTransaction.getValidWriteIds(
                ((HMSExternalCatalog) hmsTable.getCatalog()).getClient());

        return cache.getFilesByTransaction(partitions, txnValidIds, hiveTransaction.isFullAcid(), bindBrokerName);
    }

    @Override
    public List<String> getPathPartitionKeys() {
        return hmsTable.getRemoteTable().getPartitionKeys().stream()
                .map(FieldSchema::getName).filter(partitionKey -> !"".equals(partitionKey))
                .map(String::toLowerCase).collect(Collectors.toList());
    }

    @Override
    public TableIf getTargetTable() {
        return hmsTable;
    }

    @Override
    public TFileFormatType getFileFormatType() throws UserException {
        return hmsTable.getFileFormatType(sessionVariable);
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof  HiveSplit) {
            HiveSplit hiveSplit = (HiveSplit) split;
            if (hiveSplit.isACID()) {
                hiveSplit.setTableFormatType(TableFormatType.TRANSACTIONAL_HIVE);
                TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
                tableFormatFileDesc.setTableFormatType(hiveSplit.getTableFormatType().value());
                AcidInfo acidInfo = (AcidInfo) hiveSplit.getInfo();
                TTransactionalHiveDesc transactionalHiveDesc = new TTransactionalHiveDesc();
                transactionalHiveDesc.setPartition(acidInfo.getPartitionLocation());
                List<TTransactionalHiveDeleteDeltaDesc> deleteDeltaDescs = new ArrayList<>();
                for (DeleteDeltaInfo deleteDeltaInfo : acidInfo.getDeleteDeltas()) {
                    TTransactionalHiveDeleteDeltaDesc deleteDeltaDesc = new TTransactionalHiveDeleteDeltaDesc();
                    deleteDeltaDesc.setDirectoryLocation(deleteDeltaInfo.getDirectoryLocation());
                    deleteDeltaDesc.setFileNames(deleteDeltaInfo.getFileNames());
                    deleteDeltaDescs.add(deleteDeltaDesc);
                }
                transactionalHiveDesc.setDeleteDeltas(deleteDeltaDescs);
                tableFormatFileDesc.setTransactionalHiveParams(transactionalHiveDesc);
                tableFormatFileDesc.setTableLevelRowCount(-1);
                rangeDesc.setTableFormatParams(tableFormatFileDesc);
            } else {
                TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
                tableFormatFileDesc.setTableFormatType(TableFormatType.HIVE.value());
                tableFormatFileDesc.setTableLevelRowCount(-1);
                rangeDesc.setTableFormatParams(tableFormatFileDesc);
            }
        }
    }

    @Override
    protected List<String> getDeleteFiles(TFileRangeDesc rangeDesc) {
        List<String> deleteFiles = new ArrayList<>();
        if (rangeDesc == null || !rangeDesc.isSetTableFormatParams()) {
            return deleteFiles;
        }
        TTableFormatFileDesc tableFormatParams = rangeDesc.getTableFormatParams();
        if (tableFormatParams == null || !tableFormatParams.isSetTransactionalHiveParams()) {
            return deleteFiles;
        }
        TTransactionalHiveDesc hiveParams = tableFormatParams.getTransactionalHiveParams();
        if (hiveParams == null || !hiveParams.isSetDeleteDeltas()) {
            return deleteFiles;
        }
        List<TTransactionalHiveDeleteDeltaDesc> deleteDeltas = hiveParams.getDeleteDeltas();
        if (deleteDeltas == null) {
            return deleteFiles;
        }
        // Format: {directory_location}/{file_name}
        for (TTransactionalHiveDeleteDeltaDesc deleteDelta : deleteDeltas) {
            if (deleteDelta != null && deleteDelta.isSetDirectoryLocation()
                    && deleteDelta.isSetFileNames() && deleteDelta.getFileNames() != null) {
                String directoryLocation = deleteDelta.getDirectoryLocation();
                for (String fileName : deleteDelta.getFileNames()) {
                    deleteFiles.add(directoryLocation + "/" + fileName);
                }
            }
        }
        return deleteFiles;
    }

    @Override
    protected Map<String, String> getLocationProperties() {
        return hmsTable.getBackendStorageProperties();
    }

    @Override
    protected TFileAttributes getFileAttributes() throws UserException {
        TFileAttributes fileAttributes = new TFileAttributes();
        Table table = hmsTable.getRemoteTable();
        // set skip header count
        // TODO: support skip footer count
        fileAttributes.setSkipLines(HiveProperties.getSkipHeaderCount(table));
        String serDeLib = table.getSd().getSerdeInfo().getSerializationLib();
        if (serDeLib.equals(HiveMetaStoreClientHelper.HIVE_TEXT_SERDE)
                || serDeLib.equals(HiveMetaStoreClientHelper.HIVE_MULTI_DELIMIT_SERDE)) {
            TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
            // set properties of LazySimpleSerDe and MultiDelimitSerDe
            // 1. set column separator (MultiDelimitSerDe supports multi-character delimiters)
            boolean supportMultiChar = serDeLib.equals(HiveMetaStoreClientHelper.HIVE_MULTI_DELIMIT_SERDE);
            textParams.setColumnSeparator(HiveProperties.getFieldDelimiter(table, supportMultiChar));
            // 2. set line delimiter
            textParams.setLineDelimiter(HiveProperties.getLineDelimiter(table));
            // 3. set mapkv delimiter
            textParams.setMapkvDelimiter(HiveProperties.getMapKvDelimiter(table));
            // 4. set collection delimiter
            textParams.setCollectionDelimiter(HiveProperties.getCollectionDelimiter(table));
            // 5. set escape delimiter
            HiveProperties.getEscapeDelimiter(table).ifPresent(d -> textParams.setEscape(d.getBytes()[0]));
            // 6. set null format
            textParams.setNullFormat(HiveProperties.getNullFormat(table));
            fileAttributes.setTextParams(textParams);
            fileAttributes.setHeaderType("");
            fileAttributes.setEnableTextValidateUtf8(
                    sessionVariable.enableTextValidateUtf8);
        } else if (serDeLib.equals(HiveMetaStoreClientHelper.HIVE_OPEN_CSV_SERDE)) {
            TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
            // set set properties of OpenCSVSerde
            // 1. set column separator
            textParams.setColumnSeparator(HiveProperties.getSeparatorChar(table));
            // 2. set line delimiter
            textParams.setLineDelimiter(HiveProperties.getLineDelimiter(table));
            // 3. set enclose char
            textParams.setEnclose(HiveProperties.getQuoteChar(table).getBytes()[0]);
            // 4. set escape char
            textParams.setEscape(HiveProperties.getEscapeChar(table).getBytes()[0]);
            // 5. set null format with empty string to make csv reader not use "\\N" to represent null
            textParams.setNullFormat("");
            fileAttributes.setTextParams(textParams);
            fileAttributes.setHeaderType("");
            if (textParams.isSetEnclose()) {
                fileAttributes.setTrimDoubleQuotes(true);
            }
            fileAttributes.setEnableTextValidateUtf8(
                    sessionVariable.enableTextValidateUtf8);
        } else if (serDeLib.equals(HiveMetaStoreClientHelper.HIVE_JSON_SERDE)) {
            TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
            textParams.setColumnSeparator("\t");
            textParams.setLineDelimiter("\n");
            fileAttributes.setTextParams(textParams);

            fileAttributes.setJsonpaths("");
            fileAttributes.setJsonRoot("");
            fileAttributes.setNumAsString(true);
            fileAttributes.setFuzzyParse(false);
            fileAttributes.setReadJsonByLine(true);
            fileAttributes.setStripOuterArray(false);
            fileAttributes.setHeaderType("");
        } else if (serDeLib.equals(HiveMetaStoreClientHelper.OPENX_JSON_SERDE)) {
            if (!sessionVariable.isReadHiveJsonInOneColumn()) {
                TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
                textParams.setColumnSeparator("\t");
                textParams.setLineDelimiter("\n");
                fileAttributes.setTextParams(textParams);

                fileAttributes.setJsonpaths("");
                fileAttributes.setJsonRoot("");
                fileAttributes.setNumAsString(true);
                fileAttributes.setFuzzyParse(false);
                fileAttributes.setReadJsonByLine(true);
                fileAttributes.setStripOuterArray(false);
                fileAttributes.setHeaderType("");

                fileAttributes.setOpenxJsonIgnoreMalformed(
                        Boolean.parseBoolean(HiveProperties.getOpenxJsonIgnoreMalformed(table)));
            } else if (sessionVariable.isReadHiveJsonInOneColumn()
                    && hmsTable.firstColumnIsString()) {
                TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
                textParams.setLineDelimiter("\n");
                textParams.setColumnSeparator("\n");
                //First, perform row splitting according to `\n`. When performing column splitting,
                // since there is no `\n`, only one column of data will be generated.
                fileAttributes.setTextParams(textParams);
                fileAttributes.setHeaderType("");
            } else {
                throw new UserException("You set read_hive_json_in_one_column = true, but the first column of table "
                        + hmsTable.getName()
                        + " is not a string column.");
            }
        } else {
            throw new UserException(
                    "unsupported hive table serde: " + serDeLib);
        }

        return fileAttributes;
    }

    @Override
    protected TFileCompressType getFileCompressType(FileSplit fileSplit) throws UserException {
        TFileCompressType compressType = super.getFileCompressType(fileSplit);
        // hadoop use lz4 blocked codec
        if (compressType == TFileCompressType.LZ4FRAME) {
            compressType = TFileCompressType.LZ4BLOCK;
        }
        return compressType;
    }
}

