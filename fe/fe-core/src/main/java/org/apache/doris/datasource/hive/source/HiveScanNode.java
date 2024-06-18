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

import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheValue;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.datasource.hive.HiveTransaction;
import org.apache.doris.datasource.hive.source.HiveSplit.HiveSplitCreator;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TFileType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Setter;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

public class HiveScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(HiveScanNode.class);

    public static final String PROP_FIELD_DELIMITER = "field.delim";
    public static final String DEFAULT_FIELD_DELIMITER = "\1"; // "\x01"
    public static final String PROP_LINE_DELIMITER = "line.delim";
    public static final String DEFAULT_LINE_DELIMITER = "\n";
    public static final String PROP_SEPARATOR_CHAR = "separatorChar";
    public static final String PROP_QUOTA_CHAR = "quoteChar";

    public static final String PROP_COLLECTION_DELIMITER_HIVE2 = "colelction.delim";
    public static final String PROP_COLLECTION_DELIMITER_HIVE3 = "collection.delim";
    public static final String DEFAULT_COLLECTION_DELIMITER = "\2";

    public static final String PROP_MAP_KV_DELIMITER = "mapkey.delim";
    public static final String DEFAULT_MAP_KV_DELIMITER = "\003";

    protected final HMSExternalTable hmsTable;
    private HiveTransaction hiveTransaction = null;

    // will only be set in Nereids, for lagency planner, it should be null
    @Setter
    private SelectedPartitions selectedPartitions = null;

    private boolean partitionInit = false;
    private List<HivePartition> prunedPartitions;
    private Iterator<HivePartition> prunedPartitionsIter;
    private int numSplitsPerPartition = NUM_SPLITS_PER_PARTITION;

    /**
     * * External file scan node for Query Hive table
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column priv
     * eg: s3 tvf
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public HiveScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "HIVE_SCAN_NODE", StatisticalType.HIVE_SCAN_NODE, needCheckColumnPriv);
        hmsTable = (HMSExternalTable) desc.getTable();
        brokerName = hmsTable.getCatalog().bindBrokerName();
    }

    public HiveScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                        StatisticalType statisticalType, boolean needCheckColumnPriv) {
        super(id, desc, planNodeName, statisticalType, needCheckColumnPriv);
        hmsTable = (HMSExternalTable) desc.getTable();
        brokerName = hmsTable.getCatalog().bindBrokerName();
    }

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();

        if (hmsTable.isHiveTransactionalTable()) {
            this.hiveTransaction = new HiveTransaction(DebugUtil.printId(ConnectContext.get().queryId()),
                    ConnectContext.get().getQualifiedUser(), hmsTable, hmsTable.isFullAcidTable());
            Env.getCurrentHiveTransactionMgr().register(hiveTransaction);
        }
    }

    protected List<HivePartition> getPartitions() throws AnalysisException {
        List<HivePartition> resPartitions = Lists.newArrayList();
        long start = System.currentTimeMillis();
        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) hmsTable.getCatalog());
        List<Type> partitionColumnTypes = hmsTable.getPartitionColumnTypes();
        if (!partitionColumnTypes.isEmpty()) {
            // partitioned table
            boolean isPartitionPruned = selectedPartitions == null ? false : selectedPartitions.isPruned;
            Collection<PartitionItem> partitionItems;
            if (!isPartitionPruned) {
                // partitionItems is null means that the partition is not pruned by Nereids,
                // so need to prune partitions here by legacy ListPartitionPrunerV2.
                HiveMetaStoreCache.HivePartitionValues hivePartitionValues = cache.getPartitionValues(
                        hmsTable.getDbName(), hmsTable.getName(), partitionColumnTypes);
                Map<Long, PartitionItem> idToPartitionItem = hivePartitionValues.getIdToPartitionItem();
                this.totalPartitionNum = idToPartitionItem.size();
                if (!conjuncts.isEmpty()) {
                    ListPartitionPrunerV2 pruner = new ListPartitionPrunerV2(idToPartitionItem,
                            hmsTable.getPartitionColumns(), columnNameToRange,
                            hivePartitionValues.getUidToPartitionRange(),
                            hivePartitionValues.getRangeToId(),
                            hivePartitionValues.getSingleColumnRangeMap(),
                            true);
                    Collection<Long> filteredPartitionIds = pruner.prune();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("hive partition fetch and prune for table {}.{} cost: {} ms",
                                hmsTable.getDbName(), hmsTable.getName(), (System.currentTimeMillis() - start));
                    }
                    partitionItems = Lists.newArrayListWithCapacity(filteredPartitionIds.size());
                    for (Long id : filteredPartitionIds) {
                        partitionItems.add(idToPartitionItem.get(id));
                    }
                } else {
                    partitionItems = idToPartitionItem.values();
                }
            } else {
                // partitions has benn pruned by Nereids, in PruneFileScanPartition,
                // so just use the selected partitions.
                this.totalPartitionNum = selectedPartitions.totalPartitionNum;
                partitionItems = selectedPartitions.selectedPartitions.values();
            }
            Preconditions.checkNotNull(partitionItems);
            this.readPartitionNum = partitionItems.size();

            // get partitions from cache
            List<List<String>> partitionValuesList = Lists.newArrayListWithCapacity(partitionItems.size());
            for (PartitionItem item : partitionItems) {
                partitionValuesList.add(
                        ((ListPartitionItem) item).getItems().get(0).getPartitionValuesAsStringListForHive());
            }
            resPartitions = cache.getAllPartitionsWithCache(hmsTable.getDbName(), hmsTable.getName(),
                    partitionValuesList);
        } else {
            // non partitioned table, create a dummy partition to save location and inputformat,
            // so that we can unify the interface.
            HivePartition dummyPartition = new HivePartition(hmsTable.getDbName(), hmsTable.getName(), true,
                    hmsTable.getRemoteTable().getSd().getInputFormat(),
                    hmsTable.getRemoteTable().getSd().getLocation(), null, Maps.newHashMap());
            this.totalPartitionNum = 1;
            this.readPartitionNum = 1;
            resPartitions.add(dummyPartition);
        }
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setGetPartitionsFinishTime();
        }
        return resPartitions;
    }

    @Override
    public List<Split> getSplits() throws UserException {
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
            getFileSplitByPartitions(cache, prunedPartitions, allFiles, bindBrokerName);
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
    public List<Split> getNextBatch(int maxBatchSize) throws UserException {
        try {
            HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                    .getMetaStoreCache((HMSExternalCatalog) hmsTable.getCatalog());
            String bindBrokerName = hmsTable.getCatalog().bindBrokerName();
            List<Split> allFiles = Lists.newArrayList();
            int numPartitions = 0;
            while (allFiles.size() < maxBatchSize && prunedPartitionsIter.hasNext()) {
                List<HivePartition> partitions = new ArrayList<>(NUM_PARTITIONS_PER_LOOP);
                for (int i = 0; i < NUM_PARTITIONS_PER_LOOP && prunedPartitionsIter.hasNext(); ++i) {
                    partitions.add(prunedPartitionsIter.next());
                    numPartitions++;
                }
                getFileSplitByPartitions(cache, partitions, allFiles, bindBrokerName);
            }
            if (allFiles.size() / numPartitions > numSplitsPerPartition) {
                numSplitsPerPartition = allFiles.size() / numPartitions;
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
    public boolean hasNext() {
        return prunedPartitionsIter.hasNext();
    }

    @Override
    public boolean isBatchMode() {
        if (!partitionInit) {
            try {
                prunedPartitions = getPartitions();
            } catch (Exception e) {
                return false;
            }
            prunedPartitionsIter = prunedPartitions.iterator();
            partitionInit = true;
        }
        int numPartitions = ConnectContext.get().getSessionVariable().getNumPartitionsInBatchMode();
        return numPartitions >= 0 && prunedPartitions.size() >= numPartitions;
    }

    @Override
    public int numApproximateSplits() {
        return numSplitsPerPartition * prunedPartitions.size();
    }

    private void getFileSplitByPartitions(HiveMetaStoreCache cache, List<HivePartition> partitions,
                                          List<Split> allFiles, String bindBrokerName) throws IOException {
        List<FileCacheValue> fileCaches;
        if (hiveTransaction != null) {
            fileCaches = getFileSplitByTransaction(cache, partitions, bindBrokerName);
        } else {
            fileCaches = cache.getFilesByPartitionsWithCache(partitions, bindBrokerName);
        }
        if (tableSample != null) {
            List<HiveMetaStoreCache.HiveFileStatus> hiveFileStatuses = selectFiles(fileCaches);
            splitAllFiles(allFiles, hiveFileStatuses);
            return;
        }
        for (HiveMetaStoreCache.FileCacheValue fileCacheValue : fileCaches) {
            // This if branch is to support old splitter, will remove later.
            if (fileCacheValue.getSplits() != null) {
                allFiles.addAll(fileCacheValue.getSplits());
            }
            if (fileCacheValue.getFiles() != null) {
                boolean isSplittable = fileCacheValue.isSplittable();
                for (HiveMetaStoreCache.HiveFileStatus status : fileCacheValue.getFiles()) {
                    allFiles.addAll(splitFile(status.getPath(), status.getBlockSize(),
                            status.getBlockLocations(), status.getLength(), status.getModificationTime(),
                            isSplittable, fileCacheValue.getPartitionValues(),
                            new HiveSplitCreator(fileCacheValue.getAcidInfo())));
                }
            }
        }
    }

    private void splitAllFiles(List<Split> allFiles,
                               List<HiveMetaStoreCache.HiveFileStatus> hiveFileStatuses) throws IOException {
        for (HiveMetaStoreCache.HiveFileStatus status : hiveFileStatuses) {
            allFiles.addAll(splitFile(status.getPath(), status.getBlockSize(),
                    status.getBlockLocations(), status.getLength(), status.getModificationTime(),
                    status.isSplittable(), status.getPartitionValues(),
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
        ValidWriteIdList validWriteIds = hiveTransaction.getValidWriteIds(
                ((HMSExternalCatalog) hmsTable.getCatalog()).getClient());
        return cache.getFilesByTransaction(partitions, validWriteIds,
            hiveTransaction.isFullAcid(), hmsTable.getId(), bindBrokerName);
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
    protected TFileType getLocationType() throws UserException {
        return getLocationType(hmsTable.getRemoteTable().getSd().getLocation());
    }

    @Override
    protected TFileType getLocationType(String location) throws UserException {
        String bindBrokerName = hmsTable.getCatalog().bindBrokerName();
        if (bindBrokerName != null) {
            return TFileType.FILE_BROKER;
        }
        return Optional.ofNullable(LocationPath.getTFileTypeForBE(location)).orElseThrow(() ->
                new DdlException("Unknown file location " + location + " for hms table " + hmsTable.getName()));
    }

    @Override
    public TFileFormatType getFileFormatType() throws UserException {
        TFileFormatType type = null;
        String inputFormatName = hmsTable.getRemoteTable().getSd().getInputFormat();
        String hiveFormat = HiveMetaStoreClientHelper.HiveFileFormat.getFormat(inputFormatName);
        if (hiveFormat.equals(HiveMetaStoreClientHelper.HiveFileFormat.PARQUET.getDesc())) {
            type = TFileFormatType.FORMAT_PARQUET;
        } else if (hiveFormat.equals(HiveMetaStoreClientHelper.HiveFileFormat.ORC.getDesc())) {
            type = TFileFormatType.FORMAT_ORC;
        } else if (hiveFormat.equals(HiveMetaStoreClientHelper.HiveFileFormat.TEXT_FILE.getDesc())) {
            type = TFileFormatType.FORMAT_CSV_PLAIN;
        }
        return type;
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException  {
        return hmsTable.getHadoopProperties();
    }

    @Override
    protected TFileAttributes getFileAttributes() throws UserException {
        TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
        java.util.Map<String, String> delimiter = hmsTable.getRemoteTable().getSd().getSerdeInfo().getParameters();
        if (delimiter.containsKey(PROP_FIELD_DELIMITER)) {
            if (delimiter.get(PROP_FIELD_DELIMITER).length() == 0) {
                textParams.setColumnSeparator(DEFAULT_FIELD_DELIMITER);
            } else {
                textParams.setColumnSeparator(delimiter.get(PROP_FIELD_DELIMITER));
            }
        } else if (delimiter.containsKey(PROP_SEPARATOR_CHAR)) {
            textParams.setColumnSeparator(delimiter.get(PROP_SEPARATOR_CHAR));
        } else {
            textParams.setColumnSeparator(DEFAULT_FIELD_DELIMITER);
        }
        if (delimiter.containsKey(PROP_QUOTA_CHAR)) {
            textParams.setEnclose(delimiter.get(PROP_QUOTA_CHAR).getBytes()[0]);
        }
        textParams.setLineDelimiter(delimiter.getOrDefault(PROP_LINE_DELIMITER, DEFAULT_LINE_DELIMITER));
        textParams.setMapkvDelimiter(delimiter.getOrDefault(PROP_MAP_KV_DELIMITER, DEFAULT_MAP_KV_DELIMITER));

        //  textParams.collection_delimiter field is map, array and struct delimiter;
        if (delimiter.get(PROP_COLLECTION_DELIMITER_HIVE2) != null) {
            textParams.setCollectionDelimiter(delimiter.get(PROP_COLLECTION_DELIMITER_HIVE2));
        } else if (delimiter.get(PROP_COLLECTION_DELIMITER_HIVE3) != null) {
            textParams.setCollectionDelimiter(delimiter.get(PROP_COLLECTION_DELIMITER_HIVE3));
        } else {
            textParams.setCollectionDelimiter(DEFAULT_COLLECTION_DELIMITER);
        }
        TFileAttributes fileAttributes = new TFileAttributes();
        fileAttributes.setTextParams(textParams);
        fileAttributes.setHeaderType("");
        if (textParams.isSet(TFileTextScanRangeParams._Fields.ENCLOSE)) {
            fileAttributes.setTrimDoubleQuotes(true);
        }
        return fileAttributes;
    }

    @Override
    public boolean pushDownAggNoGrouping(FunctionCallExpr aggExpr) {

        String aggFunctionName = aggExpr.getFnName().getFunction();
        if (aggFunctionName.equalsIgnoreCase("COUNT")) {
            return true;
        }
        return false;
    }

    @Override
    public boolean pushDownAggNoGroupingCheckCol(FunctionCallExpr aggExpr, Column col) {
        return !col.isAllowNull();
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
