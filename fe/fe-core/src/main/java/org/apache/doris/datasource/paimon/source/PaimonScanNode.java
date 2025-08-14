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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.FileFormatUtils;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalUtil;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonUtil;
import org.apache.doris.datasource.paimon.PaimonVendedCredentialsProvider;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TPaimonDeletionFileDesc;
import org.apache.doris.thrift.TPaimonFileDesc;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PaimonScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(PaimonScanNode.class);

    private static final long COUNT_WITH_PARALLEL_SPLITS = 10000;
    // The keys of incremental read params for Paimon SDK
    private static final String PAIMON_SCAN_SNAPSHOT_ID = "scan.snapshot-id";
    private static final String PAIMON_SCAN_MODE = "scan.mode";
    private static final String PAIMON_INCREMENTAL_BETWEEN = "incremental-between";
    private static final String PAIMON_INCREMENTAL_BETWEEN_SCAN_MODE = "incremental-between-scan-mode";
    private static final String PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP = "incremental-between-timestamp";
    // The keys of incremental read params for Doris Statement
    private static final String DORIS_START_SNAPSHOT_ID = "startSnapshotId";
    private static final String DORIS_END_SNAPSHOT_ID = "endSnapshotId";
    private static final String DORIS_START_TIMESTAMP = "startTimestamp";
    private static final String DORIS_END_TIMESTAMP = "endTimestamp";
    private static final String DORIS_INCREMENTAL_BETWEEN_SCAN_MODE = "incrementalBetweenScanMode";
    private static final String DEFAULT_INCREMENTAL_BETWEEN_SCAN_MODE = "auto";

    private enum SplitReadType {
        JNI,
        NATIVE,
    }

    private class SplitStat {
        SplitReadType type = SplitReadType.JNI;
        private long rowCount = 0;
        private Optional<Long> mergedRowCount = Optional.empty();
        private boolean rawFileConvertable = false;
        private boolean hasDeletionVector = false;

        public void setType(SplitReadType type) {
            this.type = type;
        }

        public void setRowCount(long rowCount) {
            this.rowCount = rowCount;
        }

        public void setMergedRowCount(long mergedRowCount) {
            this.mergedRowCount = Optional.of(mergedRowCount);
        }

        public void setRawFileConvertable(boolean rawFileConvertable) {
            this.rawFileConvertable = rawFileConvertable;
        }

        public void setHasDeletionVector(boolean hasDeletionVector) {
            this.hasDeletionVector = hasDeletionVector;
        }

        @Override
        public String toString() {
            return "SplitStat [type=" + type
                    + ", rowCount=" + rowCount
                    + ", mergedRowCount=" + (mergedRowCount.isPresent() ? mergedRowCount.get() : "NONE")
                    + ", rawFileConvertable=" + rawFileConvertable
                    + ", hasDeletionVector=" + hasDeletionVector + "]";
        }
    }

    private PaimonSource source = null;
    private List<Predicate> predicates;
    private int rawFileSplitNum = 0;
    private int paimonSplitNum = 0;
    private List<SplitStat> splitStats = new ArrayList<>();
    private String serializedTable;

    // The schema information involved in the current query process (including historical schema).
    protected ConcurrentHashMap<Long, Boolean> currentQuerySchema = new ConcurrentHashMap<>();

    public PaimonScanNode(PlanNodeId id,
                          TupleDescriptor desc,
                          boolean needCheckColumnPriv,
                          SessionVariable sv) {
        super(id, desc, "PAIMON_SCAN_NODE", StatisticalType.PAIMON_SCAN_NODE, needCheckColumnPriv, sv);
    }

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();
        source = new PaimonSource(desc);
        serializedTable = PaimonUtil.encodeObjectToString(source.getPaimonTable());
        // Todo: Get the current schema id of the table, instead of using -1.
        ExternalUtil.initSchemaInfo(params, -1L, source.getTargetTable().getColumns());
    }

    @VisibleForTesting
    public void setSource(PaimonSource source) {
        this.source = source;
    }

    @Override
    protected void convertPredicate() {
        PaimonPredicateConverter paimonPredicateConverter = new PaimonPredicateConverter(
                source.getPaimonTable().rowType());
        predicates = paimonPredicateConverter.convertToPaimonExpr(conjuncts);
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof PaimonSplit) {
            setPaimonParams(rangeDesc, (PaimonSplit) split);
        }
    }

    @Override
    protected Optional<String> getSerializedTable() {
        return Optional.of(serializedTable);
    }

    private void putHistorySchemaInfo(Long schemaId) {
        if (currentQuerySchema.putIfAbsent(schemaId, Boolean.TRUE) == null) {
            PaimonExternalTable table = (PaimonExternalTable) source.getTargetTable();
            TableSchema tableSchema = Env.getCurrentEnv().getExtMetaCacheMgr().getPaimonMetadataCache()
                    .getPaimonSchemaCacheValue(table.getOrBuildNameMapping(), schemaId).getTableSchema();
            params.addToHistorySchemaInfo(PaimonUtil.getSchemaInfo(tableSchema));
        }
    }

    private void setPaimonParams(TFileRangeDesc rangeDesc, PaimonSplit paimonSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(paimonSplit.getTableFormatType().value());
        TPaimonFileDesc fileDesc = new TPaimonFileDesc();
        org.apache.paimon.table.source.Split split = paimonSplit.getSplit();

        String fileFormat = getFileFormat(paimonSplit.getPathString());
        if (split != null) {
            // use jni reader
            rangeDesc.setFormatType(TFileFormatType.FORMAT_JNI);
            fileDesc.setPaimonSplit(PaimonUtil.encodeObjectToString(split));
            rangeDesc.setSelfSplitWeight(paimonSplit.getSelfSplitWeight());
        } else {
            // use native reader
            if (fileFormat.equals("orc")) {
                rangeDesc.setFormatType(TFileFormatType.FORMAT_ORC);
            } else if (fileFormat.equals("parquet")) {
                rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
            } else {
                throw new RuntimeException("Unsupported file format: " + fileFormat);
            }

            putHistorySchemaInfo(paimonSplit.getSchemaId());
            fileDesc.setSchemaId(paimonSplit.getSchemaId());
        }
        fileDesc.setFileFormat(fileFormat);
        fileDesc.setPaimonPredicate(PaimonUtil.encodeObjectToString(predicates));
        fileDesc.setPaimonColumnNames(source.getDesc().getSlots().stream().map(slot -> slot.getColumn().getName())
                .collect(Collectors.joining(",")));
        fileDesc.setDbName(((PaimonExternalTable) source.getTargetTable()).getDbName());
        fileDesc.setPaimonOptions(((PaimonExternalCatalog) source.getCatalog()).getPaimonOptionsMap());
        fileDesc.setTableName(source.getTargetTable().getName());
        fileDesc.setCtlId(source.getCatalog().getId());
        fileDesc.setDbId(((PaimonExternalTable) source.getTargetTable()).getDbId());
        fileDesc.setTblId(source.getTargetTable().getId());
        fileDesc.setLastUpdateTime(source.getTargetTable().getUpdateTime());
        // The hadoop conf should be same with
        // PaimonExternalCatalog.createCatalog()#getConfiguration()
        fileDesc.setHadoopConf(source.getCatalog().getCatalogProperty().getBackendStorageProperties());
        Optional<DeletionFile> optDeletionFile = paimonSplit.getDeletionFile();
        if (optDeletionFile.isPresent()) {
            DeletionFile deletionFile = optDeletionFile.get();
            TPaimonDeletionFileDesc tDeletionFile = new TPaimonDeletionFileDesc();
            // convert the deletion file uri to make sure FileReader can read it in be
            LocationPath locationPath = LocationPath.of(deletionFile.path(),
                    source.getCatalog().getCatalogProperty().getStoragePropertiesMap());
            String path = locationPath.toStorageLocation().toString();
            tDeletionFile.setPath(path);
            tDeletionFile.setOffset(deletionFile.offset());
            tDeletionFile.setLength(deletionFile.length());
            fileDesc.setDeletionFile(tDeletionFile);
        }
        if (paimonSplit.getRowCount().isPresent()) {
            tableFormatFileDesc.setTableLevelRowCount(paimonSplit.getRowCount().get());
        }
        tableFormatFileDesc.setPaimonParams(fileDesc);
        Map<String, String> partitionValues = paimonSplit.getPaimonPartitionValues();
        if (partitionValues != null) {
            List<String> fromPathKeys = new ArrayList<>();
            List<String> fromPathValues = new ArrayList<>();
            List<Boolean> fromPathIsNull = new ArrayList<>();
            for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
                fromPathKeys.add(entry.getKey());
                fromPathValues.add(entry.getValue() != null ? entry.getValue() : "");
                fromPathIsNull.add(entry.getValue() == null);
            }
            rangeDesc.setColumnsFromPathKeys(fromPathKeys);
            rangeDesc.setColumnsFromPath(fromPathValues);
            rangeDesc.setColumnsFromPathIsNull(fromPathIsNull);
        }
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        boolean forceJniScanner = sessionVariable.isForceJniScanner();
        SessionVariable.IgnoreSplitType ignoreSplitType = SessionVariable.IgnoreSplitType
                .valueOf(sessionVariable.getIgnoreSplitType());
        List<Split> splits = new ArrayList<>();
        List<Split> pushDownCountSplits = new ArrayList<>();
        long pushDownCountSum = 0;

        List<org.apache.paimon.table.source.Split> paimonSplits = getPaimonSplitFromAPI();
        List<DataSplit> dataSplits = new ArrayList<>();
        for (org.apache.paimon.table.source.Split split : paimonSplits) {
            if (!(split instanceof DataSplit)) {
                throw new UserException("PaimonSplit type should be DataSplit but got: " + split.getClass().getName());
            }
            dataSplits.add((DataSplit) split);
        }

        boolean applyCountPushdown = getPushDownAggNoGroupingOp() == TPushAggOp.COUNT;
        // Used to avoid repeatedly calculating partition info map for the same
        // partition data.
        // And for counting the number of selected partitions for this paimon table.
        Map<BinaryRow, Map<String, String>> partitionInfoMaps = new HashMap<>();
        // if applyCountPushdown is true, we can't split the DataSplit
        long realFileSplitSize = getRealFileSplitSize(applyCountPushdown ? Long.MAX_VALUE : 0);
        for (DataSplit dataSplit : dataSplits) {
            SplitStat splitStat = new SplitStat();
            splitStat.setRowCount(dataSplit.rowCount());

            BinaryRow partitionValue = dataSplit.partition();
            Map<String, String> partitionInfoMap = null;
            if (sessionVariable.isEnableRuntimeFilterPartitionPrune()) {
                // If the partition value is not in the map, we need to calculate the partition
                // info map and store it in the map.
                partitionInfoMap = partitionInfoMaps.computeIfAbsent(partitionValue, k -> {
                    return PaimonUtil.getPartitionInfoMap(
                            source.getPaimonTable(), partitionValue, sessionVariable.getTimeZone());
                });
            } else {
                partitionInfoMaps.put(partitionValue, null);
            }
            Optional<List<RawFile>> optRawFiles = dataSplit.convertToRawFiles();
            Optional<List<DeletionFile>> optDeletionFiles = dataSplit.deletionFiles();
            if (applyCountPushdown && dataSplit.mergedRowCountAvailable()) {
                splitStat.setMergedRowCount(dataSplit.mergedRowCount());
                PaimonSplit split = new PaimonSplit(dataSplit);
                split.setRowCount(dataSplit.mergedRowCount());
                if (partitionInfoMap != null) {
                    split.setPaimonPartitionValues(partitionInfoMap);
                }
                pushDownCountSplits.add(split);
                pushDownCountSum += dataSplit.mergedRowCount();
            } else if (!forceJniScanner && supportNativeReader(optRawFiles)) {
                if (ignoreSplitType == SessionVariable.IgnoreSplitType.IGNORE_NATIVE) {
                    continue;
                }
                splitStat.setType(SplitReadType.NATIVE);
                splitStat.setRawFileConvertable(true);
                List<RawFile> rawFiles = optRawFiles.get();
                for (int i = 0; i < rawFiles.size(); i++) {
                    RawFile file = rawFiles.get(i);
                    LocationPath locationPath = LocationPath.of(file.path(),
                            source.getCatalog().getCatalogProperty().getStoragePropertiesMap());
                    try {
                        List<Split> dorisSplits = FileSplitter.splitFile(
                                locationPath,
                                realFileSplitSize,
                                null,
                                file.length(),
                                -1,
                                true,
                                null,
                                PaimonSplit.PaimonSplitCreator.DEFAULT);
                        for (Split dorisSplit : dorisSplits) {
                            PaimonSplit paimonSplit = (PaimonSplit) dorisSplit;
                            paimonSplit.setSchemaId(file.schemaId());
                            paimonSplit.setPaimonPartitionValues(partitionInfoMap);
                            // try to set deletion file
                            if (optDeletionFiles.isPresent() && optDeletionFiles.get().get(i) != null) {
                                paimonSplit.setDeletionFile(optDeletionFiles.get().get(i));
                                splitStat.setHasDeletionVector(true);
                            }
                        }
                        splits.addAll(dorisSplits);
                        ++rawFileSplitNum;
                    } catch (IOException e) {
                        throw new UserException("Paimon error to split file: " + e.getMessage(), e);
                    }
                }
            } else {
                if (ignoreSplitType == SessionVariable.IgnoreSplitType.IGNORE_JNI) {
                    continue;
                }
                splits.add(new PaimonSplit(dataSplit));
                ++paimonSplitNum;
            }

            splitStats.add(splitStat);
        }

        // if applyCountPushdown is true, calcute row count for count pushdown
        if (applyCountPushdown && !pushDownCountSplits.isEmpty()) {
            if (pushDownCountSum > COUNT_WITH_PARALLEL_SPLITS) {
                int minSplits = sessionVariable.getParallelExecInstanceNum() * numBackends;
                pushDownCountSplits = pushDownCountSplits.subList(0, Math.min(pushDownCountSplits.size(), minSplits));
            } else {
                pushDownCountSplits = Collections.singletonList(pushDownCountSplits.get(0));
            }
            setPushDownCount(pushDownCountSum);
            assignCountToSplits(pushDownCountSplits, pushDownCountSum);
            splits.addAll(pushDownCountSplits);
        }

        // We need to set the target size for all splits so that we can calculate the
        // proportion of each split later.
        splits.forEach(s -> s.setTargetSplitSize(realFileSplitSize));

        this.selectedPartitionNum = partitionInfoMaps.size();
        return splits;
    }

    @VisibleForTesting
    public Map<String, String> getIncrReadParams() throws UserException {
        Map<String, String> paimonScanParams = new HashMap<>();
        if (scanParams != null && scanParams.incrementalRead()) {
            // Validate parameter combinations and get the result map
            paimonScanParams = validateIncrementalReadParams(scanParams.getMapParams());
        }
        return paimonScanParams;
    }

    @VisibleForTesting
    public List<org.apache.paimon.table.source.Split> getPaimonSplitFromAPI() throws UserException {
        if (!source.getPaimonTable().options().containsKey(CoreOptions.SCAN_SNAPSHOT_ID.key())) {
            // an empty table in PaimonSnapshotCacheValue
            return Collections.emptyList();
        }
        int[] projected = desc.getSlots().stream().mapToInt(
                        slot -> source.getPaimonTable().rowType()
                                .getFieldNames()
                                .stream()
                                .map(String::toLowerCase)
                                .collect(Collectors.toList())
                                .indexOf(slot.getColumn().getName()))
                .toArray();
        Table paimonTable = source.getPaimonTable();

        if (getScanParams() != null && getQueryTableSnapshot() != null) {
            throw new UserException("Can not specify scan params and table snapshot at same time.");
        }
        if (getQueryTableSnapshot() != null) {
            throw new UserException("Paimon table does not support table snapshot query yet.");
        }
        Map<String, String> incrReadParams = getIncrReadParams();
        paimonTable = paimonTable.copy(incrReadParams);
        ReadBuilder readBuilder = paimonTable.newReadBuilder();
        return readBuilder.withFilter(predicates)
                .withProjection(projected)
                .newScan().plan().splits();
    }

    private String getFileFormat(String path) {
        return FileFormatUtils.getFileFormatBySuffix(path).orElse(source.getFileFormatFromTableProperties());
    }

    @VisibleForTesting
    public boolean supportNativeReader(Optional<List<RawFile>> optRawFiles) {
        if (!optRawFiles.isPresent()) {
            return false;
        }
        List<String> files = optRawFiles.get().stream().map(RawFile::path).collect(Collectors.toList());
        for (String f : files) {
            String splitFileFormat = getFileFormat(f);
            if (!splitFileFormat.equals("orc") && !splitFileFormat.equals("parquet")) {
                return false;
            }
        }
        return true;
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        return TFileFormatType.FORMAT_JNI;
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        // return new ArrayList<>(source.getPaimonTable().partitionKeys());
        // Paimon is not aware of partitions and bypasses some existing logic by
        // returning an empty list
        return new ArrayList<>();
    }

    @Override
    public TableIf getTargetTable() {
        return desc.getTable();
    }

    @Override
    protected Map<String, String> getLocationProperties() {
        PaimonExternalCatalog catalog = (PaimonExternalCatalog) source.getCatalog();
        return PaimonVendedCredentialsProvider.getBackendLocationProperties(
                catalog.getCatalogProperty().getMetastoreProperties(),
                catalog.getCatalogProperty().getStoragePropertiesMap(),
                source.getPaimonTable()
        );
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder sb = new StringBuilder(super.getNodeExplainString(prefix, detailLevel));
        sb.append(String.format("%spaimonNativeReadSplits=%d/%d\n",
                prefix, rawFileSplitNum, (paimonSplitNum + rawFileSplitNum)));

        sb.append(prefix).append("predicatesFromPaimon:");
        if (predicates.isEmpty()) {
            sb.append(" NONE\n");
        } else {
            sb.append("\n");
            for (Predicate predicate : predicates) {
                sb.append(prefix).append(prefix).append(predicate).append("\n");
            }
        }

        if (detailLevel == TExplainLevel.VERBOSE) {
            sb.append(prefix).append("PaimonSplitStats: \n");
            int size = splitStats.size();
            if (size <= 4) {
                for (SplitStat splitStat : splitStats) {
                    sb.append(String.format("%s  %s\n", prefix, splitStat));
                }
            } else {
                for (int i = 0; i < 3; i++) {
                    SplitStat splitStat = splitStats.get(i);
                    sb.append(String.format("%s  %s\n", prefix, splitStat));
                }
                int other = size - 4;
                sb.append(prefix).append("  ... other ").append(other).append(" paimon split stats ...\n");
                SplitStat split = splitStats.get(size - 1);
                sb.append(String.format("%s  %s\n", prefix, split));
            }
        }
        return sb.toString();
    }

    private void assignCountToSplits(List<Split> splits, long totalCount) {
        int size = splits.size();
        long countPerSplit = totalCount / size;
        for (int i = 0; i < size - 1; i++) {
            ((PaimonSplit) splits.get(i)).setRowCount(countPerSplit);
        }
        ((PaimonSplit) splits.get(size - 1)).setRowCount(countPerSplit + totalCount % size);
    }

    @VisibleForTesting
    public static Map<String, String> validateIncrementalReadParams(Map<String, String> params) throws UserException {
        // Check if snapshot-based parameters exist
        boolean hasStartSnapshotId = params.containsKey(DORIS_START_SNAPSHOT_ID)
                && params.get(DORIS_START_SNAPSHOT_ID) != null;
        boolean hasEndSnapshotId = params.containsKey(DORIS_END_SNAPSHOT_ID)
                && params.get(DORIS_END_SNAPSHOT_ID) != null;
        boolean hasIncrementalBetweenScanMode = params.containsKey(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE)
                && params.get(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE) != null;

        // Check if timestamp-based parameters exist
        boolean hasStartTimestamp = params.containsKey(DORIS_START_TIMESTAMP)
                && params.get(DORIS_START_TIMESTAMP) != null;
        boolean hasEndTimestamp = params.containsKey(DORIS_END_TIMESTAMP) && params.get(DORIS_END_TIMESTAMP) != null;

        // Check if any snapshot-based parameters are present
        boolean hasSnapshotParams = hasStartSnapshotId || hasEndSnapshotId || hasIncrementalBetweenScanMode;

        // Check if any timestamp-based parameters are present
        boolean hasTimestampParams = hasStartTimestamp || hasEndTimestamp;

        // Rule 2: The two groups are mutually exclusive
        if (hasSnapshotParams && hasTimestampParams) {
            throw new UserException(
                    "Cannot specify both snapshot-based parameters"
                            + "(startSnapshotId, endSnapshotId, incrementalBetweenScanMode) "
                            + "and timestamp-based parameters (startTimestamp, endTimestamp) at the same time");
        }

        // Validate snapshot-based parameters group
        if (hasSnapshotParams) {
            // Rule 3.1 & 3.2: DORIS_START_SNAPSHOT_ID is required
            if (!hasStartSnapshotId) {
                throw new UserException("startSnapshotId is required when using snapshot-based incremental read");
            }

            // Rule 3.3: DORIS_INCREMENTAL_BETWEEN_SCAN_MODE can only appear
            // when both start and end snapshot IDs are specified
            if (hasIncrementalBetweenScanMode && (!hasStartSnapshotId || !hasEndSnapshotId)) {
                throw new UserException(
                        "incrementalBetweenScanMode can only be specified when"
                                + " both startSnapshotId and endSnapshotId are provided");
            }

            // Validate snapshot ID values
            if (hasStartSnapshotId) {
                try {
                    long startSId = Long.parseLong(params.get(DORIS_START_SNAPSHOT_ID));
                    if (startSId <= 0) {
                        throw new UserException("startSnapshotId must be greater than 0");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid startSnapshotId format: " + e.getMessage());
                }
            }

            if (hasEndSnapshotId) {
                try {
                    long endSId = Long.parseLong(params.get(DORIS_END_SNAPSHOT_ID));
                    if (endSId <= 0) {
                        throw new UserException("endSnapshotId must be greater than 0");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid endSnapshotId format: " + e.getMessage());
                }
            }

            // Check if both snapshot IDs are present and validate their relationship
            if (hasStartSnapshotId && hasEndSnapshotId) {
                try {
                    long startSId = Long.parseLong(params.get(DORIS_START_SNAPSHOT_ID));
                    long endSId = Long.parseLong(params.get(DORIS_END_SNAPSHOT_ID));
                    if (startSId >= endSId) {
                        throw new UserException("startSnapshotId must be less than endSnapshotId");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid snapshot ID format: " + e.getMessage());
                }
            }

            // Validate DORIS_INCREMENTAL_BETWEEN_SCAN_MODE
            if (hasIncrementalBetweenScanMode) {
                String scanMode = params.get(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE).toLowerCase();
                if (!scanMode.equals("auto") && !scanMode.equals("diff")
                        && !scanMode.equals("delta") && !scanMode.equals("changelog")) {
                    throw new UserException("incrementalBetweenScanMode must be one of: auto, diff, delta, changelog");
                }
            }
        }

        // Validate timestamp-based parameters group
        if (hasTimestampParams) {
            // Rule 4.1 & 4.2: DORIS_START_TIMESTAMP is required
            if (!hasStartTimestamp) {
                throw new UserException("startTimestamp is required when using timestamp-based incremental read");
            }

            // Validate timestamp values
            if (hasStartTimestamp) {
                try {
                    long startTS = Long.parseLong(params.get(DORIS_START_TIMESTAMP));
                    if (startTS < 0) {
                        throw new UserException("startTimestamp must be greater than or equal to 0");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid startTimestamp format: " + e.getMessage());
                }
            }

            if (hasEndTimestamp) {
                try {
                    long endTS = Long.parseLong(params.get(DORIS_END_TIMESTAMP));
                    if (endTS <= 0) {
                        throw new UserException("endTimestamp must be greater than 0");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid endTimestamp format: " + e.getMessage());
                }
            }

            // Check if both timestamps are present and validate their relationship
            if (hasStartTimestamp && hasEndTimestamp) {
                try {
                    long startTS = Long.parseLong(params.get(DORIS_START_TIMESTAMP));
                    long endTS = Long.parseLong(params.get(DORIS_END_TIMESTAMP));
                    if (startTS >= endTS) {
                        throw new UserException("startTimestamp must be less than endTimestamp");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid timestamp format: " + e.getMessage());
                }
            }
        }

        // If no incremental parameters are provided at all, that's also invalid in this context
        if (!hasSnapshotParams && !hasTimestampParams) {
            throw new UserException(
                    "Invalid paimon incremental read params: at least one valid parameter group must be specified");
        }

        // Fill the result map based on parameter combinations
        Map<String, String> paimonScanParams = new HashMap<>();
        paimonScanParams.put(PAIMON_SCAN_SNAPSHOT_ID, null);
        paimonScanParams.put(PAIMON_SCAN_MODE, null);

        if (hasSnapshotParams) {
            paimonScanParams.put(PAIMON_SCAN_MODE, null);
            if (hasStartSnapshotId && !hasEndSnapshotId) {
                // Only startSnapshotId is specified
                throw new UserException("endSnapshotId is required when using snapshot-based incremental read");
            } else if (hasStartSnapshotId && hasEndSnapshotId) {
                // Both start and end snapshot IDs are specified
                String startSId = params.get(DORIS_START_SNAPSHOT_ID);
                String endSId = params.get(DORIS_END_SNAPSHOT_ID);
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN, startSId + "," + endSId);
            }

            // Add incremental between scan mode if present
            if (hasIncrementalBetweenScanMode) {
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN_SCAN_MODE,
                        params.get(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE));
            }
        }

        if (hasTimestampParams) {
            String startTS = params.get(DORIS_START_TIMESTAMP);
            String endTS = params.get(DORIS_END_TIMESTAMP);

            if (hasStartTimestamp && !hasEndTimestamp) {
                // Only startTimestamp is specified
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP, startTS + "," + Long.MAX_VALUE);
            } else if (hasStartTimestamp && hasEndTimestamp) {
                // Both start and end timestamps are specified
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP, startTS + "," + endTS);
            }
        }

        return paimonScanParams;
    }
}


