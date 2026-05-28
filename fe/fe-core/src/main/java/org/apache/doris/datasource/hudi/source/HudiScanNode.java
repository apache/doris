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
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.HMSPartitionsUtil;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.ExternalUtil;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.datasource.hive.source.HiveScanNode;
import org.apache.doris.datasource.hudi.HudiSchemaCacheValue;
import org.apache.doris.datasource.hudi.HudiUtils;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.property.metastore.HMSBaseProperties;
import org.apache.doris.fs.DirectoryLister;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.BDPAuthContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.THudiFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.storage.HoodieStorageStrategy;
import org.apache.hudi.common.storage.HoodieStorageStrategyFactory;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.hadoop.client.HoodieHadoopTable;
import org.apache.hudi.hadoop.client.HoodieSplit;
import org.apache.hudi.hadoop.client.HoodieSplit.SerializableInputSplit;
import org.apache.hudi.hadoop.config.InputFormatConfig;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class HudiScanNode extends HiveScanNode {

    private static final Logger LOG = LogManager.getLogger(HudiScanNode.class);

    private static final String HOODIE_TABLE_LOG_FORMAT = "hoodie.table.log.format";
    private static final String HOODIE_LOG_FORMAT_LSM = "lsm";
    private static final int HUDI_SPLIT_FORK_JOIN_PARALLELISM =
            Math.max(1, ForkJoinPool.getCommonPoolParallelism());
    private static final ForkJoinPool HUDI_SPLIT_FORK_JOIN_POOL = new ForkJoinPool(
            HUDI_SPLIT_FORK_JOIN_PARALLELISM,
            new LegacyForkJoinWorkerThreadFactory(),
            null,
            false);

    private boolean isCowTable;

    private final AtomicLong noLogsSplitNum = new AtomicLong(0);

    private HoodieTableMetaClient hudiClient;
    private String basePath;
    private String inputFormat;
    private String serdeLib;
    private List<String> columnNames;
    private List<String> columnTypes;
    private List<String> primaryKeys;

    private boolean partitionInit = false;
    private HoodieTimeline timeline;
    private String queryInstant;

    private final AtomicReference<UserException> batchException = new AtomicReference<>(null);
    private List<HivePartition> prunedPartitions;
    private final Semaphore splittersOnFlight = new Semaphore(NUM_SPLITTERS_ON_FLIGHT);
    private final AtomicInteger numSplitsPerPartition = new AtomicInteger(NUM_SPLITS_PER_PARTITION);

    private boolean incrementalRead = false;
    private boolean enableCdcRead = false;
    private TableScanParams scanParams;
    private IncrementalRelation incrementalRelation;
    private HoodieTableFileSystemView fsView;
    private HoodieStorageStrategy storageStrategy;

    private String chubaoFsOwner;

    // The schema information involved in the current query process (including historical schema).
    protected ConcurrentHashMap<Long, Boolean> currentQuerySchema = new ConcurrentHashMap<>();

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
            SessionVariable sv, DirectoryLister directoryLister, ScanContext scanContext) {
        super(id, desc, "HUDI_SCAN_NODE", StatisticalType.HUDI_SCAN_NODE,
                needCheckColumnPriv, sv, directoryLister, scanContext);
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
        if (this.incrementalRead) {
            this.enableCdcRead = this.scanParams == null || this.scanParams.isCdcRead();
        }
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
        hudiClient = HiveMetaStoreClientHelper.getHudiClient(hmsTable);
        hudiClient.reloadActiveTimeline();
        basePath = hmsTable.getRemoteTable().getSd().getLocation();
        inputFormat = hmsTable.getRemoteTable().getSd().getInputFormat();
        serdeLib = hmsTable.getRemoteTable().getSd().getSerdeInfo().getSerializationLib();
        chubaoFsOwner = hudiClient.getTableConfig().getChubaoFsOwner();
        if (chubaoFsOwner != null && chubaoFsOwner.contains(":")) {
            chubaoFsOwner = chubaoFsOwner.split(":")[1];
        }
        boolean queryWithoutCacheLayer = false;
        Map<String, String> storageDescriptorParameters =
                hmsTable.getRemoteTable().getSd().getSerdeInfo().getParameters();

        Map<String, String> tableParams = hmsTable.getRemoteTable().getParameters();
        String key = "hoodie.query.without.cache.layer.enabled";
        if (tableParams != null) {
            queryWithoutCacheLayer = Boolean.valueOf(tableParams.getOrDefault(key, "false"));
        }
        if (!queryWithoutCacheLayer && storageDescriptorParameters != null) {
            queryWithoutCacheLayer = Boolean.valueOf(storageDescriptorParameters.getOrDefault(key, "false"));
        }
        String primaryKey = "hoodie.datasource.write.recordkey.field";
        String primaryKeyStr = tableParams != null ? tableParams.getOrDefault(primaryKey, "") : "";
        TypedProperties props = hudiClient.getTableConfig().getProps();
        String primaryKeyStrHoodie = props.getString(HoodieTableConfig.RECORDKEY_FIELDS.key());
        String effectivePrimaryKey = StringUtils.isNullOrEmpty(primaryKeyStr) ? primaryKeyStrHoodie : primaryKeyStr;
        if (StringUtils.isNullOrEmpty(effectivePrimaryKey)) {
            throw new AnalysisException("Primary key is not set in hudi table " + hmsTable.getFullQualifiers());
        }
        primaryKeys = Arrays.asList(effectivePrimaryKey.split(","));
        storageStrategy = HoodieStorageStrategyFactory.getInstant(hudiClient, queryWithoutCacheLayer);

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

        timeline = hudiClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants();
        TableSnapshot tableSnapshot = getQueryTableSnapshot();
        if (tableSnapshot != null) {
            if (tableSnapshot.getType() == TableSnapshot.VersionType.VERSION) {
                throw new UserException("Hudi does not support `FOR VERSION AS OF`, please use `FOR TIME AS OF`");
            }
            queryInstant = tableSnapshot.getValue().replaceAll("[-: ]", "");
        } else {
            Option<HoodieInstant> snapshotInstant = timeline.lastInstant();
            if (!snapshotInstant.isPresent()) {
                prunedPartitions = Collections.emptyList();
                partitionInit = true;
                return;
            }
            queryInstant = snapshotInstant.get().getTimestamp();
        }

        HudiSchemaCacheValue hudiSchemaCacheValue = HudiUtils.getSchemaCacheValue(hmsTable, queryInstant);
        columnNames = hudiSchemaCacheValue.getSchema().stream().map(Column::getName).collect(Collectors.toList());
        columnTypes = hudiSchemaCacheValue.getColTypes();

        fsView = Env.getCurrentEnv()
            .getExtMetaCacheMgr()
            .getFsViewProcessor(hmsTable.getCatalog())
            .getFsView(hmsTable.getDbName(), hmsTable.getName(), hudiClient);
        // Todo: Get the current schema id of the table, instead of using -1.
        // In Be Parquet/Rrc reader, if `current table schema id == current file schema id`, then its
        // `table_info_node_ptr` will be `TableSchemaChangeHelper::ConstNode`. When using `ConstNode`,
        // you need to pay special attention to the `case difference` between the `table column name`
        // and `the file column name`.
        ExternalUtil.initSchemaInfo(params, -1L, table.getColumns());
    }

    @Override
    protected Map<String, String> getLocationProperties() {
        Map<String, String> properties = new HashMap<>();
        if (incrementalRead) {
            properties.putAll(incrementalRelation.getHoodieParams());
            properties.put(InputFormatConfig.ENABLE_CDC_READ, String.valueOf(enableCdcRead));
        } else {
            properties.putAll(hmsTable.getBackendStorageProperties());
            properties.putAll(hmsTable.getCatalog().getCatalogProperty().getHadoopProperties());
        }
        return properties;
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof HudiSplit) {
            setHudiParams(rangeDesc, (HudiSplit) split);
        }
    }


    private void putHistorySchemaInfo(InternalSchema internalSchema) {
        if (currentQuerySchema.putIfAbsent(internalSchema.schemaId(), Boolean.TRUE) == null) {
            params.addToHistorySchemaInfo(HudiUtils.getSchemaInfo(internalSchema));
        }
    }

    private void setHudiParams(TFileRangeDesc rangeDesc, HudiSplit hudiSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(hudiSplit.getTableFormatType().value());
        THudiFileDesc fileDesc = new THudiFileDesc();

        // Check if this is a new reader split with serialized input split
        if (hudiSplit.getSerializedInputSplit() != null) {
            fileDesc.setSerializedInputSplit(hudiSplit.getSerializedInputSplit());
            fileDesc.setDatabaseName(hudiSplit.getDatabaseName());
            fileDesc.setTableName(hudiSplit.getTableName());
            fileDesc.setStartInstant(hudiSplit.getStartInstant());
            fileDesc.setEndInstant(hudiSplit.getEndInstant());
            fileDesc.setInstantTime(hudiSplit.getInstantTime());
            fileDesc.setSerde(hudiSplit.getSerde());
            fileDesc.setInputFormat(hudiSplit.getInputFormat());
            fileDesc.setBasePath(hudiSplit.getBasePath());
            fileDesc.setDataFilePath(hudiSplit.getDataFilePath());
            fileDesc.setDataFileLength(hudiSplit.getFileLength());
            fileDesc.setDeltaLogs(hudiSplit.getHudiDeltaLogs());
            fileDesc.setColumnNames(hudiSplit.getHudiColumnNames());
            fileDesc.setColumnTypes(hudiSplit.getHudiColumnTypes());
            fileDesc.setPrimaryKeys(hudiSplit.getPrimaryKeys());
            if (hudiSplit.getSerializedHoodieTable() != null && !hudiSplit.getSerializedHoodieTable().isEmpty()) {
                fileDesc.setSerializedHoodieTable(hudiSplit.getSerializedHoodieTable());
            }
        } else {
            // Old reader path
            fileDesc.setInstantTime(hudiSplit.getInstantTime());
            fileDesc.setSerde(hudiSplit.getSerde());
            fileDesc.setInputFormat(hudiSplit.getInputFormat());
            fileDesc.setBasePath(hudiSplit.getBasePath());
            fileDesc.setDataFilePath(hudiSplit.getDataFilePath());
            fileDesc.setDataFileLength(hudiSplit.getFileLength());
            fileDesc.setDeltaLogs(hudiSplit.getHudiDeltaLogs());
            fileDesc.setColumnNames(hudiSplit.getHudiColumnNames());
            fileDesc.setColumnTypes(hudiSplit.getHudiColumnTypes());
            fileDesc.setPrimaryKeys(hudiSplit.getPrimaryKeys());
        }
        // TODO(gaoxin): support complex types
        // fileDesc.setNestedFields(hudiSplit.getNestedFields());

        // Native HudiParquetReader matches split schema_id against params.current_schema_id /
        // history_schema_info. Unset schema_id defaults to 0 in BE and causes
        // "miss table/file schema info, file_schema_idx:-1".
        if (params.isSetCurrentSchemaId()) {
            fileDesc.setSchemaId(params.getCurrentSchemaId());
        }

        tableFormatFileDesc.setHudiParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    private void setSharedSerializedHoodieTable(String serializedHoodieTable) {
        if (serializedHoodieTable == null || serializedHoodieTable.isEmpty()) {
            return;
        }
        TTableFormatFileDesc sharedTableFormat = new TTableFormatFileDesc();
        sharedTableFormat.setTableFormatType(TableFormatType.HUDI.value());
        THudiFileDesc sharedHudiDesc = new THudiFileDesc();
        sharedHudiDesc.setSerializedHoodieTable(serializedHoodieTable);
        sharedTableFormat.setHudiParams(sharedHudiDesc);
        params.setTableFormatParams(sharedTableFormat);
    }

    private boolean canUseNativeReader() {
        return !sessionVariable.isForceJniScanner() && isCowTable;
    }

    private List<HivePartition> getPrunedPartitions(HoodieTableMetaClient metaClient) {
        NameMapping nameMapping = hmsTable.getOrBuildNameMapping();
        List<Type> partitionColumnTypes = hmsTable.getPartitionColumnTypes(MvccUtil.getSnapshotFromContext(hmsTable));
        if (!partitionColumnTypes.isEmpty()) {
            this.totalPartitionNum = selectedPartitions.totalPartitionNum;
            Map<String, PartitionItem> prunedPartitions = selectedPartitions.selectedPartitions;
            this.selectedPartitionNum = prunedPartitions.size();

            String inputFormat = hmsTable.getRemoteTable().getSd().getInputFormat();
            String basePath = metaClient.getBasePath().toString();

            List<HivePartition> hivePartitions = Lists.newArrayList();
            prunedPartitions.forEach(
                    (key, value) -> {
                        String path = basePath + "/" + key;
                        hivePartitions.add(new HivePartition(
                                nameMapping, false, inputFormat, path,
                                ((ListPartitionItem) value).getItems().get(0).getPartitionValuesAsStringList(),
                                Maps.newHashMap()));
                    }
            );
            return hivePartitions;
        }
        // unpartitioned table, create a dummy partition to save location and
        // inputformat,
        // so that we can unify the interface.
        HivePartition dummyPartition = new HivePartition(nameMapping, true,
                hmsTable.getRemoteTable().getSd().getInputFormat(),
                hmsTable.getRemoteTable().getSd().getLocation(), null, Maps.newHashMap());
        this.totalPartitionNum = 1;
        this.selectedPartitionNum = 1;
        return Lists.newArrayList(dummyPartition);
    }

    // Data class to hold partition metadata
    private static class PartitionMetadata {
        final String partitionName;
        final HivePartition partition;
        long totalSize;

        PartitionMetadata(String partitionName, HivePartition partition, long totalSize) {
            this.partitionName = partitionName;
            this.partition = partition;
            this.totalSize = totalSize;
        }
    }

    private void getPartitionSplits(HivePartition partition, List<Split> splits) throws IOException {

        String partitionName;
        if (partition.isDummyPartition()) {
            partitionName = "";
        } else {
            partitionName = FSUtils.getRelativePartitionPath(new Path(hudiClient.getBasePath()),
                    new Path(partition.getPath()));
        }

        if (canUseNativeReader()) {
            addCowNativeReaderSplits(fsView, partitionName, partition, splits, null);
        } else {
            fsView.getLatestMergedFileSlicesBeforeOrOn(partitionName, queryInstant)
                    .forEach(fileSlice -> splits.add(
                            generateHudiSplit(fileSlice, partition.getPartitionValues(), queryInstant)));
        }
    }

    private PartitionMetadata getPartitionMetadata(HivePartition partition, Path basePath) {
        String partitionName;
        if (partition.isDummyPartition()) {
            partitionName = "";
        } else {
            partitionName = FSUtils.getRelativePartitionPath(basePath,
                new Path(partition.getPath()));
        }
        return new PartitionMetadata(partitionName, partition, 0);
    }

    /**
     * Add COW base-file splits for native Parquet reader. Must use {@link HudiSplit} so BE receives
     * table_format_params.hudi (see #841 getPartitionsSplits regression).
     */
    private void addCowNativeReaderSplits(HoodieTableFileSystemView fileSystemView, String partitionName,
            HivePartition partition, List<Split> splits, PartitionMetadata metadata) {
        final Map<String, String> partitionValues = sessionVariable.isEnableRuntimeFilterPartitionPrune()
                ? HudiUtils.getPartitionInfoMap(hmsTable, partition)
                : null;
        fileSystemView.getLatestBaseFilesBeforeOrOn(partitionName, queryInstant).forEach(baseFile -> {
            noLogsSplitNum.incrementAndGet();
            String filePath = baseFile.getPath();
            long fileSize = baseFile.getFileSize();
            if (metadata != null) {
                metadata.totalSize += fileSize;
            }
            LocationPath locationPath = LocationPath.of(filePath, hmsTable.getStoragePropertiesMap());
            HudiSplit hudiSplit = new HudiSplit(locationPath, 0, fileSize, fileSize,
                    new String[0], partition.getPartitionValues());
            hudiSplit.setTableFormatType(TableFormatType.HUDI);
            if (partitionValues != null) {
                hudiSplit.setHudiPartitionValues(partitionValues);
            }
            splits.add(hudiSplit);
        });
    }

    private void processPartitionWithMetadata(HoodieTableFileSystemView fileSystemView,
                                              PartitionMetadata metadata, List<Split> splits) {
        if (canUseNativeReader()) {
            addCowNativeReaderSplits(fileSystemView, metadata.partitionName, metadata.partition, splits,
                    metadata);
        } else {
            fileSystemView.getLatestMergedFileSlicesBeforeOrOn(metadata.partitionName, queryInstant)
                    .forEach(fileSlice -> {
                        splits.add(
                                generateHudiSplit(fileSlice, metadata.partition.getPartitionValues(), queryInstant));
                        metadata.totalSize += fileSlice.getTotalFileSize();
                    });
        }
    }

    public void getPartitionsSplits(List<HivePartition> partitions, List<Split> splits) throws AnalysisException {
        ExecutorService executor = Env.getCurrentEnv().getExtMetaCacheMgr().getLakehouseGetPartitionSplitExecutor();
        List<PartitionMetadata> metadataList = Collections.synchronizedList(new ArrayList<>());
        Path basePath = hudiClient.getBasePathV2();
        for (HivePartition partition : partitions) {
            PartitionMetadata metadata = getPartitionMetadata(partition, basePath);
            metadataList.add(metadata);
        }
        CountDownLatch countDownLatch = new CountDownLatch(metadataList.size());
        AtomicReference<Throwable> error = new AtomicReference<>();
        HoodieTableFileSystemView fileSystemView = new HoodieTableFileSystemView(hudiClient,
                timeline, storageStrategy);
        List<Future<?>> futures = Lists.newArrayList();
        metadataList.forEach(metadata -> {
            Future<?> f = executor.submit(() -> {
                try {
                    processPartitionWithMetadata(fileSystemView, metadata, splits);
                    HMSPartitionsUtil.checkSelectedSplitNumLimit(hmsTable, splits.size());
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                } finally {
                    countDownLatch.countDown();
                }
            });
            futures.add(f);
        });

        try {
            if (!countDownLatch.await(Config.lakehouse_get_split_max_second, TimeUnit.SECONDS)) {
                for (Future<?> f : futures) {
                    f.cancel(true);
                }
                throw new AnalysisException("Timeout while processing partitions for hudi table: "
                    + hmsTable.getDbName() + "." + hmsTable.getName());
            }
        } catch (InterruptedException e) {
            throw new AnalysisException("Interrupted while processing partitions: " + e.getMessage(), e);
        }

        if (error.get() != null) {
            Throwable t = error.get();
            if (t instanceof AnalysisException) {
                throw (AnalysisException) t;
            }
            throw new AnalysisException("Failed to process partitions: " + t.getMessage(), t);
        }

        long totalFileSize = 0L;
        for (PartitionMetadata metadata : metadataList) {
            totalFileSize += metadata.totalSize;
        }
        if (totalFileSize > sessionVariable.maxSelectedTotalFileSizeForLakehouseTable) {
            throw new AnalysisException("the total scan bytes: " + totalFileSize
                + " for " + hmsTable.getDbName() + "." + hmsTable.getName()
                + " has exceed max bytes for single hive/hudi table: "
                + sessionVariable.maxSelectedTotalFileSizeForLakehouseTable);
        }
        MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.increase(totalFileSize);
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        // @incr queries use new-reader path (HoodieHadoopTable.getSplits + serialized input splits).
        if (incrementalRead) {
            List<Split> splits = getNewHudiSplits();
            if (LOG.isInfoEnabled()) {
                boolean hasSerialized = false;
                if (!splits.isEmpty() && splits.get(0) instanceof HudiSplit) {
                    HudiSplit hs = (HudiSplit) splits.get(0);
                    hasSerialized = hs.getSerializedInputSplit() != null && !hs.getSerializedInputSplit().isEmpty();
                }
                LOG.info("Hudi @incr getSplits: table={}, incrementalRead={}, enableCdcRead={}, splits={},"
                        + "firstHasSerializedInputSplit={}, query_id={}",
                        hmsTable.getFullQualifiers(), incrementalRead, enableCdcRead, splits.size(), hasSerialized);
            }
            return splits;
        }
        // LSM log-format (non-COW): same 2.1 path — getNewHudiSplits → JDHadoopHudiJniScanner on BE.
        if (isHudiLsmMorTable()) {
            return getNewHudiSplits();
        }
        if (!partitionInit) {
            try {
                prunedPartitions = hmsTable.getCatalog().getExecutionAuthenticator().execute(()
                        -> getPrunedPartitions(hudiClient));
            } catch (Exception e) {
                throw new UserException(ExceptionUtils.getRootCauseMessage(e), e);
            }
            partitionInit = true;
        }
        List<Split> splits = Collections.synchronizedList(new ArrayList<>());
        try {
            hmsTable.getCatalog().getExecutionAuthenticator().execute(() -> {
                getPartitionsSplits(prunedPartitions, splits);
                return null;
            });
        } catch (Exception e) {
            throw new UserException(ExceptionUtils.getRootCauseMessage(e), e);
        }
        long totalFileSize = 0L;
        for (Split split : splits) {
            totalFileSize += split.getLength();
        }
        MetricRepo.COUNTER_HMS_SCAN_SIZE_BYTES.increase(totalFileSize);
        return splits;
    }

    @Override
    public void startSplit(int numBackends) {
        if (prunedPartitions.isEmpty()) {
            splitAssignment.finishSchedule();
            return;
        }
        AtomicInteger numFinishedPartitions = new AtomicInteger(0);
        ExecutorService scheduleExecutor = Env.getCurrentEnv().getExtMetaCacheMgr().getScheduleExecutor();
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
            }
            if (batchException.get() != null) {
                splitAssignment.setException(batchException.get());
            }
        }, scheduleExecutor);
    }

    @Override
    public boolean isBatchMode() {
        if (incrementalRead && !incrementalRelation.fallbackFullTableScan()) {
            return false;
        }
        // Lazy split scheduling uses the legacy per-partition path; LSM uses synchronous getNewHudiSplits.
        if (isHudiLsmMorTable()) {
            return false;
        }
        if (!partitionInit) {
            // Non partition table will get one dummy partition
            try {
                prunedPartitions = hmsTable.getCatalog().getExecutionAuthenticator().execute(()
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
                .map(Path::toString)
                .collect(Collectors.toList());
        if (logs.isEmpty() && !sessionVariable.isForceJniScanner()) {
            noLogsSplitNum.incrementAndGet();
        }

        // no base file, use log file to parse file type
        String agencyPath = filePath.isEmpty() ? logs.get(0) : filePath;
        LocationPath locationPath = LocationPath.of(agencyPath, hmsTable.getStoragePropertiesMap());
        HudiSplit split = new HudiSplit(locationPath,
                0, fileSize, fileSize, new String[0], partitionValues);
        split.setTableFormatType(TableFormatType.HUDI);
        split.setDataFilePath(filePath);
        split.setHudiDeltaLogs(logs);
        split.setInputFormat(inputFormat);
        split.setSerde(serdeLib);
        split.setBasePath(basePath);
        split.setHudiColumnNames(columnNames);
        split.setHudiColumnTypes(columnTypes);
        split.setPrimaryKeys(primaryKeys);
        split.setInstantTime(queryInstant);
        return split;
    }

    /**
     * True when the table is <b>not</b> COW and HMS marks LSM log format ({@code hoodie.table.log.format=lsm}).
     * COW is excluded up front; the remaining case is typically MOR, so this was historically called
     * "LSM MOR" — the rule is really "LSM layout + non-COW", not an extra MOR predicate.
     */
    private boolean isHudiLsmMorTable() {
        if (hmsTable.isHoodieCowTable()) {
            return false;
        }
        org.apache.hadoop.hive.metastore.api.Table remoteTable = hmsTable.getRemoteTable();
        if (remoteTable == null) {
            return false;
        }
        Map<String, String> params = remoteTable.getParameters();
        if (params == null) {
            return false;
        }
        String logFormat = params.get(HOODIE_TABLE_LOG_FORMAT);
        return logFormat != null && HOODIE_LOG_FORMAT_LSM.equalsIgnoreCase(logFormat.trim());
    }

    /**
     * Generate splits using new HoodieHadoopTable.getSplits API
     */
    private List<Split> getNewHudiSplits() throws UserException {
        try {
            if (!partitionInit) {
                prunedPartitions = getPartitions();
                partitionInit = true;
            }
            TableSnapshot tableSnapshot = getQueryTableSnapshot();
            String startInstantValue = null;
            String endInstantValue = null;

            // Always take instants from incrementalRelation when present (including full-table fallback),
            // so LSM HoodieHadoopTable.getSplits never sees a null start when @incr is used.
            final boolean hasIncrRelation = incrementalRead && incrementalRelation != null;
            if (hasIncrRelation) {
                startInstantValue = incrementalRelation.getStartTs();
                endInstantValue = incrementalRelation.getEndTs();
            }
            if (incrementalRead && (startInstantValue == null || startInstantValue.isEmpty()) && scanParams != null) {
                for (Map.Entry<String, String> e : scanParams.getMapParams().entrySet()) {
                    if (e.getKey() == null) {
                        continue;
                    }
                    String lk = e.getKey().toLowerCase(Locale.ROOT);
                    if ("begintime".equals(lk)) {
                        startInstantValue = e.getValue();
                    } else if ("endtime".equals(lk)) {
                        endInstantValue = e.getValue();
                    }
                }
            }

            // Time travel / non-@incr: do not overwrite @incr(start,end) after relation/scanParams above.
            if (!incrementalRead) {
                if (tableSnapshot != null) {
                    if (tableSnapshot.getType() == TableSnapshot.VersionType.TIME) {
                        startInstantValue = tableSnapshot.getValue().replaceAll("[-: ]", "");
                    } else {
                        throw new UserException("New Hudi reader only supports TIME snapshot type");
                    }
                } else if (queryInstant != null) {
                    endInstantValue = queryInstant;
                }
                if (startInstantValue == null || startInstantValue.isEmpty()) {
                    startInstantValue = "000";
                }
            } else if (startInstantValue == null || startInstantValue.isEmpty()) {
                throw new UserException("Hudi @incr read requires beginTime (start instant is empty)");
            }

            final String startInstant = startInstantValue;
            final String endInstant = endInstantValue;

            // Build Hadoop configuration
            Configuration conf = new Configuration();
            Map<String, String> hadoopProperties = hmsTable.getBackendStorageProperties();
            for (Map.Entry<String, String> entry : hadoopProperties.entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
            }
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            conf.setBoolean("hoodie.hive.parquet.read_as_avro", true);
            // HoodieHadoopTable depends on hive metastore uris, must not be null.
            String metastoreUris = null;
            if (hadoopProperties != null) {
                metastoreUris = hadoopProperties.get(HMSBaseProperties.HIVE_METASTORE_URIS);
            }
            if (metastoreUris == null) {
                metastoreUris = hmsTable.getCatalogProperties().get(HMSBaseProperties.HIVE_METASTORE_URIS);
            }
            if (metastoreUris == null && params != null && params.properties != null) {
                metastoreUris = params.properties.get(HMSBaseProperties.HIVE_METASTORE_URIS);
            }
            Preconditions.checkArgument(metastoreUris != null,
                    "The value of property hive.metastore.uris must not be null");
            conf.set(HMSBaseProperties.HIVE_METASTORE_URIS, metastoreUris);

            BDPAuthContext bdpAuthContext = BDPAuthContext.get();
            conf.setStrings("BEE_SOURCE", bdpAuthContext.getSource());
            conf.setStrings("BEE_USER", bdpAuthContext.getErp());
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(bdpAuthContext.getHadoopUserName(), null,
                    bdpAuthContext.getUserToken());
            // Create HoodieHadoopTable
            HoodieHadoopTable table = ugi.doAs((PrivilegedAction<HoodieHadoopTable>) () -> {
                try {
                    return new HoodieHadoopTable(hmsTable.getDbName(), hmsTable.getName(), conf);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            // Build filter from pruned partitions.
            String filter = buildPartitionFilter(prunedPartitions);

            // Build options (see getLocationProperties for CDC toggle rules)
            Map<String, String> options = new HashMap<>();
            options.put(InputFormatConfig.ENABLE_CDC_READ, String.valueOf(enableCdcRead));
            if (hasIncrRelation) {
                options.putAll(incrementalRelation.getHoodieParams());
            }
            LOG.info("Going to get splits for table: {}, filter: {}", hmsTable.getName(), filter);
            // Get splits in shared ForkJoinPool to avoid creating a new pool per query.
            HoodieSplit hoodieSplit = ugi.doAs((PrivilegedAction<HoodieSplit>) () -> {
                try {
                    return HUDI_SPLIT_FORK_JOIN_POOL.submit(() -> {
                        try {
                            return table.getSplits(startInstant, endInstant, filter, options);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new RuntimeException(cause);
                }
            });

            SerializableInputSplit[] serializableInputSplits = hoodieSplit.serializedInputSplit();
            LOG.info("Found {} splits for table: {}, filter: {}",
                    serializableInputSplits.length, hmsTable.getName(), filter);
            String serializedHoodieTable = encodeObjectToString(table);
            setSharedSerializedHoodieTable(serializedHoodieTable);

            // Convert to HudiSplit list
            List<Split> splits = new ArrayList<>();
            for (SerializableInputSplit serializableSplit : serializableInputSplits) {
                // Serialize the split
                String serializedSplit = encodeObjectToString(serializableSplit);

                // Use the Hadoop split file path so FileQueryScanNode can fill columns_from_path
                // for partitioned tables (base path alone has no key=value segments).
                String splitLocation = basePath;
                try {
                    Object wrapped = serializableSplit.get();
                    if (wrapped instanceof org.apache.hadoop.mapred.FileSplit) {
                        Path p = ((org.apache.hadoop.mapred.FileSplit) wrapped).getPath();
                        if (p != null) {
                            splitLocation = p.toString();
                        }
                    } else if (wrapped instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
                        Path p = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) wrapped).getPath();
                        if (p != null) {
                            splitLocation = p.toString();
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("Could not get path from SerializableInputSplit, using table base path: {}",
                            e.getMessage());
                }

                HudiSplit hudiSplit = new HudiSplit(
                        LocationPath.of(splitLocation, hmsTable.getStoragePropertiesMap()),
                        0, 0, 0, new String[0], Collections.emptyList());
                hudiSplit.setTableFormatType(TableFormatType.HUDI);
                hudiSplit.setSerializedInputSplit(serializedSplit);
                // BE HudiJniReader passes serialized_hoodie_table from per-range THudiFileDesc, not scan params.
                hudiSplit.setSerializedHoodieTable(serializedHoodieTable);
                hudiSplit.setDatabaseName(hmsTable.getDbName());
                hudiSplit.setTableName(hmsTable.getName());
                hudiSplit.setStartInstant(startInstant);
                hudiSplit.setEndInstant(endInstant != null ? endInstant : hoodieSplit.endInstant());
                // Same table schema/metadata as generateHudiSplit — JNI requires hudi_column_names / types, etc.
                hudiSplit.setHudiColumnNames(columnNames);
                hudiSplit.setHudiColumnTypes(columnTypes);
                hudiSplit.setPrimaryKeys(primaryKeys);
                hudiSplit.setSerde(serdeLib);
                hudiSplit.setInputFormat(inputFormat);
                hudiSplit.setBasePath(basePath);
                hudiSplit.setInstantTime(queryInstant);

                splits.add(hudiSplit);
            }

            return splits;
        } catch (Exception e) {
            throw new UserException("Failed to generate new Hudi splits: " + e.getMessage(), e);
        }
    }

    private String buildPartitionFilter(List<HivePartition> partitions) {
        if (partitions == null || partitions.isEmpty()) {
            return "";
        }

        Option<String[]> partitionColumns = hudiClient.getTableConfig().getPartitionFields();
        if (partitionColumns == null || !partitionColumns.isPresent()) {
            return "";
        }
        List<String> partitionNames = Arrays.asList(partitionColumns.get());
        if (partitionNames.isEmpty()) {
            return "";
        }

        List<String> partitionPredicates = new ArrayList<>();
        for (HivePartition partition : partitions) {
            if (partition == null || partition.isDummyPartition()) {
                continue;
            }
            List<String> values = partition.getPartitionValues();
            if (values == null || values.size() != partitionNames.size()) {
                continue;
            }
            List<String> conjunctPredicates = new ArrayList<>(partitionNames.size());
            for (int i = 0; i < partitionNames.size(); i++) {
                String partitionName = partitionNames.get(i);
                if (partitionName == null || partitionName.isEmpty()) {
                    conjunctPredicates.clear();
                    break;
                }
                String partitionValue = values.get(i);
                String escapedValue = partitionValue == null ? HiveMetaStoreCache.HIVE_DEFAULT_PARTITION
                        : partitionValue.replace("'", "''");
                conjunctPredicates.add(partitionName.toLowerCase(Locale.ROOT) + "='" + escapedValue + "'");
            }
            if (!conjunctPredicates.isEmpty()) {
                partitionPredicates.add("(" + String.join(" and ", conjunctPredicates) + ")");
            }
        }

        return partitionPredicates.isEmpty() ? "" : String.join(" or ", partitionPredicates);
    }

    private String encodeObjectToString(Object object) {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(object);
            objectOutputStream.close();

            return Base64.getUrlEncoder().encodeToString(byteArrayOutputStream.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException("Failed to encode object: " + e.getMessage(), e);
        }
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
