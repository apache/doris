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

package org.apache.doris.planner.external;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheValue;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.datasource.hive.HiveTransaction;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.external.HiveSplit.HiveSplitCreator;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HiveScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(HiveScanNode.class);

    public static final String PROP_FIELD_DELIMITER = "field.delim";
    public static final String DEFAULT_FIELD_DELIMITER = "\1"; // "\x01"
    public static final String DEFAULT_LINE_DELIMITER = "\n";

    protected final HMSExternalTable hmsTable;
    private HiveTransaction hiveTransaction = null;

    /**
     * * External file scan node for Query Hive table
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column priv
     * eg: s3 tvf
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public HiveScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "HIVE_SCAN_NODE", StatisticalType.HIVE_SCAN_NODE, needCheckColumnPriv);
        hmsTable = (HMSExternalTable) desc.getTable();
    }

    public HiveScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                        StatisticalType statisticalType, boolean needCheckColumnPriv) {
        super(id, desc, planNodeName, statisticalType, needCheckColumnPriv);
        hmsTable = (HMSExternalTable) desc.getTable();
    }

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();
        genSlotToSchemaIdMap();
        String inputFormat = hmsTable.getRemoteTable().getSd().getInputFormat();
        if (inputFormat.contains("TextInputFormat")) {
            for (SlotDescriptor slot : desc.getSlots()) {
                if (!slot.getType().isScalarType()) {
                    throw new UserException("For column `" + slot.getColumn().getName()
                            + "`, The column types ARRAY/MAP/STRUCT are not supported yet"
                            + " for text input format of Hive. ");
                }
            }
        }

        if (hmsTable.isHiveTransactionalTable()) {
            this.hiveTransaction = new HiveTransaction(DebugUtil.printId(ConnectContext.get().queryId()),
                    ConnectContext.get().getQualifiedUser(), hmsTable, hmsTable.isFullAcidTable());
            Env.getCurrentHiveTransactionMgr().register(hiveTransaction);
        }
    }

    protected List<HivePartition> getPartitions() throws AnalysisException {
        long start = System.currentTimeMillis();
        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) hmsTable.getCatalog());
        // 1. get ListPartitionItems from cache
        HiveMetaStoreCache.HivePartitionValues hivePartitionValues = null;
        List<Type> partitionColumnTypes = hmsTable.getPartitionColumnTypes();
        if (!partitionColumnTypes.isEmpty()) {
            hivePartitionValues = cache.getPartitionValues(hmsTable.getDbName(), hmsTable.getName(),
                    partitionColumnTypes);
        }
        if (hivePartitionValues != null) {
            // 2. prune partitions by expr
            Map<Long, PartitionItem> idToPartitionItem = hivePartitionValues.getIdToPartitionItem();
            this.totalPartitionNum = idToPartitionItem.size();
            ListPartitionPrunerV2 pruner = new ListPartitionPrunerV2(idToPartitionItem,
                    hmsTable.getPartitionColumns(), columnNameToRange,
                    hivePartitionValues.getUidToPartitionRange(),
                    hivePartitionValues.getRangeToId(),
                    hivePartitionValues.getSingleColumnRangeMap(),
                    true);
            Collection<Long> filteredPartitionIds = pruner.prune();
            this.readPartitionNum = filteredPartitionIds.size();
            LOG.debug("hive partition fetch and prune for table {}.{} cost: {} ms",
                    hmsTable.getDbName(), hmsTable.getName(), (System.currentTimeMillis() - start));

            // 3. get partitions from cache
            List<List<String>> partitionValuesList = Lists.newArrayListWithCapacity(filteredPartitionIds.size());
            for (Long id : filteredPartitionIds) {
                ListPartitionItem listPartitionItem = (ListPartitionItem) idToPartitionItem.get(id);
                partitionValuesList.add(listPartitionItem.getItems().get(0).getPartitionValuesAsStringList());
            }
            return cache.getAllPartitions(hmsTable.getDbName(), hmsTable.getName(), partitionValuesList);
        } else {
            // unpartitioned table, create a dummy partition to save location and inputformat,
            // so that we can unify the interface.
            HivePartition dummyPartition = new HivePartition(hmsTable.getDbName(), hmsTable.getName(), true,
                    hmsTable.getRemoteTable().getSd().getInputFormat(),
                    hmsTable.getRemoteTable().getSd().getLocation(), null);
            this.totalPartitionNum = 1;
            this.readPartitionNum = 1;
            return Lists.newArrayList(dummyPartition);
        }
    }

    @Override
    protected List<Split> getSplits() throws UserException {
        long start = System.currentTimeMillis();
        try {
            HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                    .getMetaStoreCache((HMSExternalCatalog) hmsTable.getCatalog());
            boolean useSelfSplitter = hmsTable.getCatalog().useSelfSplitter();
            List<Split> allFiles = Lists.newArrayList();
            getFileSplitByPartitions(cache, getPartitions(), allFiles, useSelfSplitter);
            LOG.debug("get #{} files for table: {}.{}, cost: {} ms",
                    allFiles.size(), hmsTable.getDbName(), hmsTable.getName(), (System.currentTimeMillis() - start));
            return allFiles;
        } catch (Throwable t) {
            LOG.warn("get file split failed for table: {}", hmsTable.getName(), t);
            throw new UserException(
                "get file split failed for table: " + hmsTable.getName() + ", err: " + Util.getRootCauseMessage(t),
                t);
        }
    }

    private void getFileSplitByPartitions(HiveMetaStoreCache cache, List<HivePartition> partitions,
                                          List<Split> allFiles, boolean useSelfSplitter) throws IOException {
        List<FileCacheValue> fileCaches;
        if (hiveTransaction != null) {
            fileCaches = getFileSplitByTransaction(cache, partitions);
        } else {
            fileCaches = cache.getFilesByPartitions(partitions, useSelfSplitter);
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

    private List<FileCacheValue> getFileSplitByTransaction(HiveMetaStoreCache cache, List<HivePartition> partitions) {
        for (HivePartition partition : partitions) {
            if (partition.getPartitionValues() == null || partition.getPartitionValues().isEmpty()) {
                // this is unpartitioned table.
                continue;
            }
            hiveTransaction.addPartition(partition.getPartitionName(hmsTable.getPartitionColumns()));
        }
        ValidWriteIdList validWriteIds = hiveTransaction.getValidWriteIds(
                ((HMSExternalCatalog) hmsTable.getCatalog()).getClient());
        return cache.getFilesByTransaction(partitions, validWriteIds, hiveTransaction.isFullAcid());
    }

    @Override
    public List<String> getPathPartitionKeys() {
        return hmsTable.getRemoteTable().getPartitionKeys()
                .stream().map(FieldSchema::getName).map(String::toLowerCase).collect(Collectors.toList());
    }

    @Override
    public TableIf getTargetTable() {
        return hmsTable;
    }

    @Override
    protected TFileType getLocationType() throws UserException {
        String location = hmsTable.getRemoteTable().getSd().getLocation();
        return getTFileType(location).orElseThrow(() ->
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
        return hmsTable.getCatalogProperties();
    }

    @Override
    protected TFileAttributes getFileAttributes() throws UserException {
        TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
        textParams.setColumnSeparator(hmsTable.getRemoteTable().getSd().getSerdeInfo().getParameters()
                .getOrDefault(PROP_FIELD_DELIMITER, DEFAULT_FIELD_DELIMITER));
        textParams.setLineDelimiter(DEFAULT_LINE_DELIMITER);
        TFileAttributes fileAttributes = new TFileAttributes();
        fileAttributes.setTextParams(textParams);
        fileAttributes.setHeaderType("");
        return fileAttributes;
    }

    // To Support Hive 1.x orc internal column name like (_col0, _col1, _col2...)
    private void genSlotToSchemaIdMap() {
        List<Column> baseSchema = desc.getTable().getBaseSchema();
        Map<String, Integer> columnNameToPosition = Maps.newHashMap();
        for (SlotDescriptor slot : desc.getSlots()) {
            int idx = 0;
            for (Column col : baseSchema) {
                if (col.getName().equals(slot.getColumn().getName())) {
                    columnNameToPosition.put(col.getName(), idx);
                    break;
                }
                idx += 1;
            }
        }
        params.setSlotNameToSchemaPos(columnNameToPosition);
    }
}
