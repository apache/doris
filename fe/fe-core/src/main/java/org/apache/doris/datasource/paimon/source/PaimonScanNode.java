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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.FileFormatUtils;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.utils.InstantiationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PaimonScanNode extends FileQueryScanNode {
    private enum SplitReadType {
        JNI,
        NATIVE,
    }

    private class SplitStat {
        SplitReadType type = SplitReadType.JNI;
        private long rowCount = 0;
        private boolean rawFileConvertable = false;
        private boolean hasDeletionVector = false;

        public void setType(SplitReadType type) {
            this.type = type;
        }

        public void setRowCount(long rowCount) {
            this.rowCount = rowCount;
        }

        public void setRawFileConvertable(boolean rawFileConvertable) {
            this.rawFileConvertable = rawFileConvertable;
        }

        public void setHasDeletionVector(boolean hasDeletionVector) {
            this.hasDeletionVector = hasDeletionVector;
        }

        @Override
        public String toString() {
            return "SplitStat [type=" + type + ", rowCount=" + rowCount + ", rawFileConvertable=" + rawFileConvertable
                    + ", hasDeletionVector=" + hasDeletionVector + "]";
        }
    }

    private static final Logger LOG = LogManager.getLogger(PaimonScanNode.class);
    private PaimonSource source = null;
    private List<Predicate> predicates;
    private int rawFileSplitNum = 0;
    private int paimonSplitNum = 0;
    private List<SplitStat> splitStats = new ArrayList<>();
    private String serializedTable;

    private boolean pushDownCount = false;
    private static final long COUNT_WITH_PARALLEL_SPLITS = 10000;

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
        serializedTable = encodeObjectToString(source.getPaimonTable());
        Preconditions.checkNotNull(source);
    }

    @Override
    protected void convertPredicate() {
        PaimonPredicateConverter paimonPredicateConverter = new PaimonPredicateConverter(
                source.getPaimonTable().rowType());
        predicates = paimonPredicateConverter.convertToPaimonExpr(conjuncts);
    }

    private static final Base64.Encoder BASE64_ENCODER = java.util.Base64.getUrlEncoder().withoutPadding();

    public static <T> String encodeObjectToString(T t) {
        try {
            byte[] bytes = InstantiationUtil.serializeObject(t);
            return new String(BASE64_ENCODER.encode(bytes), java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    private void setPaimonParams(TFileRangeDesc rangeDesc, PaimonSplit paimonSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(paimonSplit.getTableFormatType().value());
        TPaimonFileDesc fileDesc = new TPaimonFileDesc();
        org.apache.paimon.table.source.Split split = paimonSplit.getSplit();

        String fileFormat = getFileFormat(paimonSplit.getPathString());
        if (split != null) {
            // use jni reader
            rangeDesc.setFormatType(TFileFormatType.FORMAT_JNI);
            fileDesc.setPaimonSplit(encodeObjectToString(split));
        } else {
            // use native reader
            if (fileFormat.equals("orc")) {
                rangeDesc.setFormatType(TFileFormatType.FORMAT_ORC);
            } else if (fileFormat.equals("parquet")) {
                rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
            } else {
                throw new RuntimeException("Unsupported file format: " + fileFormat);
            }
        }

        fileDesc.setFileFormat(fileFormat);
        fileDesc.setPaimonPredicate(encodeObjectToString(predicates));
        fileDesc.setPaimonColumnNames(source.getDesc().getSlots().stream().map(slot -> slot.getColumn().getName())
                .collect(Collectors.joining(",")));
        fileDesc.setDbName(((PaimonExternalTable) source.getTargetTable()).getDbName());
        fileDesc.setPaimonOptions(((PaimonExternalCatalog) source.getCatalog()).getPaimonOptionsMap());
        fileDesc.setTableName(source.getTargetTable().getName());
        fileDesc.setCtlId(source.getCatalog().getId());
        fileDesc.setDbId(((PaimonExternalTable) source.getTargetTable()).getDbId());
        fileDesc.setTblId(source.getTargetTable().getId());
        fileDesc.setLastUpdateTime(source.getTargetTable().getUpdateTime());
        // The hadoop conf should be same with PaimonExternalCatalog.createCatalog()#getConfiguration()
        fileDesc.setHadoopConf(source.getCatalog().getCatalogProperty().getHadoopProperties());
        Optional<DeletionFile> optDeletionFile = paimonSplit.getDeletionFile();
        if (optDeletionFile.isPresent()) {
            DeletionFile deletionFile = optDeletionFile.get();
            TPaimonDeletionFileDesc tDeletionFile = new TPaimonDeletionFileDesc();
            tDeletionFile.setPath(deletionFile.path());
            tDeletionFile.setOffset(deletionFile.offset());
            tDeletionFile.setLength(deletionFile.length());
            fileDesc.setDeletionFile(tDeletionFile);
        }
        tableFormatFileDesc.setPaimonParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        boolean forceJniScanner = sessionVariable.isForceJniScanner();
        SessionVariable.IgnoreSplitType ignoreSplitType = SessionVariable.IgnoreSplitType
                .valueOf(sessionVariable.getIgnoreSplitType());
        List<Split> splits = new ArrayList<>();
        int[] projected = desc.getSlots().stream().mapToInt(
                slot -> (source.getPaimonTable().rowType().getFieldNames().indexOf(slot.getColumn().getName())))
                .toArray();
        ReadBuilder readBuilder = source.getPaimonTable().newReadBuilder();
        List<org.apache.paimon.table.source.Split> paimonSplits = readBuilder.withFilter(predicates)
                .withProjection(projected)
                .newScan().plan().splits();

        boolean applyCountPushdown = getPushDownAggNoGroupingOp() == TPushAggOp.COUNT;
        // Just for counting the number of selected partitions for this paimon table
        Set<BinaryRow> selectedPartitionValues = Sets.newHashSet();
        for (org.apache.paimon.table.source.Split split : paimonSplits) {
            SplitStat splitStat = new SplitStat();
            splitStat.setRowCount(split.rowCount());
            if (!forceJniScanner && split instanceof DataSplit) {
                DataSplit dataSplit = (DataSplit) split;
                BinaryRow partitionValue = dataSplit.partition();
                selectedPartitionValues.add(partitionValue);
                Optional<List<RawFile>> optRawFiles = dataSplit.convertToRawFiles();
                Optional<List<DeletionFile>> optDeletionFiles = dataSplit.deletionFiles();

                if (supportNativeReader(optRawFiles)) {
                    if (ignoreSplitType == SessionVariable.IgnoreSplitType.IGNORE_NATIVE) {
                        continue;
                    }
                    splitStat.setType(SplitReadType.NATIVE);
                    splitStat.setRawFileConvertable(true);
                    List<RawFile> rawFiles = optRawFiles.get();
                    if (optDeletionFiles.isPresent()) {
                        List<DeletionFile> deletionFiles = optDeletionFiles.get();
                        for (int i = 0; i < rawFiles.size(); i++) {
                            RawFile file = rawFiles.get(i);
                            DeletionFile deletionFile = deletionFiles.get(i);
                            LocationPath locationPath = new LocationPath(file.path(),
                                    source.getCatalog().getProperties());
                            try {
                                List<Split> dorisSplits = FileSplitter.splitFile(
                                        locationPath,
                                        getRealFileSplitSize(0),
                                        null,
                                        file.length(),
                                        -1,
                                        true,
                                        null,
                                        PaimonSplit.PaimonSplitCreator.DEFAULT);
                                for (Split dorisSplit : dorisSplits) {
                                    // the element in DeletionFiles might be null
                                    if (deletionFile != null) {
                                        splitStat.setHasDeletionVector(true);
                                        ((PaimonSplit) dorisSplit).setDeletionFile(deletionFile);
                                    }
                                    splits.add(dorisSplit);
                                }
                                ++rawFileSplitNum;
                            } catch (IOException e) {
                                throw new UserException("Paimon error to split file: " + e.getMessage(), e);
                            }
                        }
                    } else {
                        createRawFileSplits(rawFiles, splits, applyCountPushdown ? Long.MAX_VALUE : 0);
                    }
                } else {
                    if (ignoreSplitType == SessionVariable.IgnoreSplitType.IGNORE_JNI) {
                        continue;
                    }
                    splits.add(new PaimonSplit(split));
                    ++paimonSplitNum;
                }
            } else {
                if (ignoreSplitType == SessionVariable.IgnoreSplitType.IGNORE_JNI) {
                    continue;
                }
                splits.add(new PaimonSplit(split));
                ++paimonSplitNum;
            }
            splitStats.add(splitStat);
        }

        this.selectedPartitionNum = selectedPartitionValues.size();
        // TODO: get total partition number
        return splits;
    }

    private void createRawFileSplits(List<RawFile> rawFiles, List<Split> splits, long blockSize) throws UserException {
        for (RawFile file : rawFiles) {
            LocationPath locationPath = new LocationPath(file.path(),
                    source.getCatalog().getProperties());
            try {
                splits.addAll(
                        FileSplitter.splitFile(
                                locationPath,
                                getRealFileSplitSize(blockSize),
                                null,
                                file.length(),
                                -1,
                                true,
                                null,
                                PaimonSplit.PaimonSplitCreator.DEFAULT));
                ++rawFileSplitNum;
            } catch (IOException e) {
                throw new UserException("Paimon error to split file: " + e.getMessage(), e);
            }
        }
    }

    private String getFileFormat(String path) {
        return FileFormatUtils.getFileFormatBySuffix(path).orElse(source.getFileFormatFromTableProperties());
    }

    private boolean supportNativeReader(Optional<List<RawFile>> optRawFiles) {
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
        // Paymon is not aware of partitions and bypasses some existing logic by
        // returning an empty list
        return new ArrayList<>();
    }

    @Override
    public TableIf getTargetTable() {
        return desc.getTable();
    }

    @Override
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        HashMap<String, String> map = new HashMap<>(source.getCatalog().getProperties());
        source.getCatalog().getCatalogProperty().getHadoopProperties().forEach((k, v) -> {
            if (!map.containsKey(k)) {
                map.put(k, v);
            }
        });
        return map;
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
}
