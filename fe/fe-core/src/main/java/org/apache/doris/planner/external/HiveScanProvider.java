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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.HivePartitionValues;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.planner.ColumnRange;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.external.ExternalFileScanNode.ParamCreateContext;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TFileType;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A HiveScanProvider to get information for scan node.
 */
public class HiveScanProvider extends HMSTableScanProvider {
    private static final Logger LOG = LogManager.getLogger(HiveScanProvider.class);

    private static final String PROP_FIELD_DELIMITER = "field.delim";
    private static final String DEFAULT_FIELD_DELIMITER = "\1"; // "\x01"
    private static final String DEFAULT_LINE_DELIMITER = "\n";

    protected HMSExternalTable hmsTable;
    protected final TupleDescriptor desc;
    protected Map<String, ColumnRange> columnNameToRange;

    protected int totalPartitionNum = 0;
    protected int readPartitionNum = 0;

    public HiveScanProvider(HMSExternalTable hmsTable, TupleDescriptor desc,
            Map<String, ColumnRange> columnNameToRange) {
        this.hmsTable = hmsTable;
        this.desc = desc;
        this.columnNameToRange = columnNameToRange;
    }

    @Override
    public TableIf getTargetTable() {
        return hmsTable;
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        TFileFormatType type = null;
        String inputFormatName = getRemoteHiveTable().getSd().getInputFormat();
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
    public TFileType getLocationType() throws DdlException, MetaNotFoundException {
        String location = hmsTable.getRemoteTable().getSd().getLocation();
        if (location != null && !location.isEmpty()) {
            if (location.startsWith(FeConstants.FS_PREFIX_S3)
                    || location.startsWith(FeConstants.FS_PREFIX_S3A)
                    || location.startsWith(FeConstants.FS_PREFIX_S3N)
                    || location.startsWith(FeConstants.FS_PREFIX_BOS)
                    || location.startsWith(FeConstants.FS_PREFIX_COS)
                    || location.startsWith(FeConstants.FS_PREFIX_OSS)
                    || location.startsWith(FeConstants.FS_PREFIX_OBS)) {
                return TFileType.FILE_S3;
            } else if (location.startsWith(FeConstants.FS_PREFIX_HDFS)) {
                return TFileType.FILE_HDFS;
            } else if (location.startsWith(FeConstants.FS_PREFIX_FILE)) {
                return TFileType.FILE_LOCAL;
            }
        }
        throw new DdlException("Unknown file location " + location + " for hms table " + hmsTable.getName());
    }

    @Override
    public String getMetaStoreUrl() {
        return hmsTable.getMetastoreUri();
    }

    @Override
    public List<InputSplit> getSplits(List<Expr> exprs) throws UserException {
        long start = System.currentTimeMillis();
        try {
            HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                    .getMetaStoreCache((HMSExternalCatalog) hmsTable.getCatalog());
            // 1. get ListPartitionItems from cache
            HivePartitionValues hivePartitionValues = null;
            List<Type> partitionColumnTypes = hmsTable.getPartitionColumnTypes();
            if (!partitionColumnTypes.isEmpty()) {
                hivePartitionValues = cache.getPartitionValues(hmsTable.getDbName(), hmsTable.getName(),
                        partitionColumnTypes);
            }

            List<InputSplit> allFiles = Lists.newArrayList();
            if (hivePartitionValues != null) {
                // 2. prune partitions by expr
                Map<Long, PartitionItem> idToPartitionItem = hivePartitionValues.getIdToPartitionItem();
                this.totalPartitionNum = idToPartitionItem.size();
                ListPartitionPrunerV2 pruner = new ListPartitionPrunerV2(idToPartitionItem,
                        hmsTable.getPartitionColumns(), columnNameToRange,
                        hivePartitionValues.getUidToPartitionRange(),
                        hivePartitionValues.getRangeToId(),
                        hivePartitionValues.getSingleColumnRangeMap());
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
                List<HivePartition> partitions = cache.getAllPartitions(hmsTable.getDbName(), hmsTable.getName(),
                        partitionValuesList);
                // 4. get all files of partitions
                getFileSplitByPartitions(cache, partitions, allFiles);
            } else {
                // unpartitioned table, create a dummy partition to save location and inputformat,
                // so that we can unify the interface.
                HivePartition dummyPartition = new HivePartition(hmsTable.getRemoteTable().getSd().getInputFormat(),
                        hmsTable.getRemoteTable().getSd().getLocation(), null);
                getFileSplitByPartitions(cache, Lists.newArrayList(dummyPartition), allFiles);
                this.totalPartitionNum = 1;
                this.readPartitionNum = 1;
            }
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
            List<InputSplit> allFiles) {
        List<InputSplit> files = cache.getFilesByPartitions(partitions);
        if (LOG.isDebugEnabled()) {
            LOG.debug("get #{} files from #{} partitions: {}: {}", files.size(), partitions.size(),
                    Joiner.on(",")
                            .join(files.stream().limit(10).map(f -> ((FileSplit) f).getPath())
                                    .collect(Collectors.toList())));
        }
        allFiles.addAll(files);
    }

    protected Configuration setConfiguration() {
        Configuration conf = new HdfsConfiguration();
        for (Map.Entry<String, String> entry : hmsTable.getCatalog().getCatalogProperty().getProperties().entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        Map<String, String> s3Properties = hmsTable.getS3Properties();
        for (Map.Entry<String, String> entry : s3Properties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }

    public int getTotalPartitionNum() {
        return totalPartitionNum;
    }

    public int getReadPartitionNum() {
        return readPartitionNum;
    }

    @Override
    public Table getRemoteHiveTable() throws DdlException, MetaNotFoundException {
        return hmsTable.getRemoteTable();
    }

    @Override
    public Map<String, String> getTableProperties() throws MetaNotFoundException {
        // TODO: implement it when we really properties from remote table.
        return Maps.newHashMap();
    }

    @Override
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        TFileType locationType = getLocationType();
        if (locationType == TFileType.FILE_S3) {
            return hmsTable.getS3Properties();
        } else if (locationType == TFileType.FILE_HDFS) {
            return hmsTable.getDfsProperties();
        } else {
            return Maps.newHashMap();
        }
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return getRemoteHiveTable().getPartitionKeys().stream().map(FieldSchema::getName).collect(Collectors.toList());
    }

    @Override
    public ParamCreateContext createContext(Analyzer analyzer) throws UserException {
        ParamCreateContext context = new ParamCreateContext();
        context.params = new TFileScanRangeParams();
        context.destTupleDescriptor = desc;
        context.params.setDestTupleId(desc.getId().asInt());
        context.fileGroup = new BrokerFileGroup(hmsTable.getId(),
                hmsTable.getRemoteTable().getSd().getLocation(), hmsTable.getRemoteTable().getSd().getInputFormat());


        // Hive table must extract partition value from path and hudi/iceberg table keep
        // partition field in file.
        List<String> partitionKeys = getPathPartitionKeys();
        List<Column> columns = hmsTable.getBaseSchema(false);
        context.params.setNumOfColumnsFromFile(columns.size() - partitionKeys.size());
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }

            TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
            slotInfo.setSlotId(slot.getId().asInt());
            slotInfo.setIsFileSlot(!partitionKeys.contains(slot.getColumn().getName()));
            context.params.addToRequiredSlots(slotInfo);
        }
        return context;
    }

    @Override
    public TFileAttributes getFileAttributes() throws UserException {
        TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
        textParams.setColumnSeparator(hmsTable.getRemoteTable().getSd().getSerdeInfo().getParameters()
                .getOrDefault(PROP_FIELD_DELIMITER, DEFAULT_FIELD_DELIMITER));
        textParams.setLineDelimiter(DEFAULT_LINE_DELIMITER);
        TFileAttributes fileAttributes = new TFileAttributes();
        fileAttributes.setTextParams(textParams);
        fileAttributes.setHeaderType("");
        return fileAttributes;
    }
}


