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

import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPaimonDeletionFileDesc;
import org.apache.doris.thrift.TPaimonFileDesc;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
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

    public PaimonScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "PAIMON_SCAN_NODE", StatisticalType.PAIMON_SCAN_NODE, needCheckColumnPriv);
    }

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();
        source = new PaimonSource(desc);
        Preconditions.checkNotNull(source);
        PaimonPredicateConverter paimonPredicateConverter = new PaimonPredicateConverter(
                source.getPaimonTable().rowType());
        predicates = paimonPredicateConverter.convertToPaimonExpr(conjuncts);
    }

    private static final Base64.Encoder BASE64_ENCODER =
            java.util.Base64.getUrlEncoder().withoutPadding();

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

    public void setPaimonParams(TFileRangeDesc rangeDesc, PaimonSplit paimonSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(paimonSplit.getTableFormatType().value());
        TPaimonFileDesc fileDesc = new TPaimonFileDesc();
        org.apache.paimon.table.source.Split split = paimonSplit.getSplit();
        if (split != null) {
            // use jni reader
            fileDesc.setPaimonSplit(encodeObjectToString(split));
        }
        fileDesc.setFileFormat(source.getFileFormat());
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
    public List<Split> getSplits() throws UserException {
        boolean forceJniScanner = ConnectContext.get().getSessionVariable().isForceJniScanner();
        List<Split> splits = new ArrayList<>();
        int[] projected = desc.getSlots().stream().mapToInt(
                slot -> (source.getPaimonTable().rowType().getFieldNames().indexOf(slot.getColumn().getName())))
                .toArray();
        ReadBuilder readBuilder = source.getPaimonTable().newReadBuilder();
        List<org.apache.paimon.table.source.Split> paimonSplits = readBuilder.withFilter(predicates)
                .withProjection(projected)
                .newScan().plan().splits();
        boolean supportNative = supportNativeReader();
        // Just for counting the number of selected partitions for this paimon table
        Set<BinaryRow> selectedPartitionValues = Sets.newHashSet();
        for (org.apache.paimon.table.source.Split split : paimonSplits) {
            SplitStat splitStat = new SplitStat();
            splitStat.setRowCount(split.rowCount());
            if (!forceJniScanner && supportNative && split instanceof DataSplit) {
                DataSplit dataSplit = (DataSplit) split;
                BinaryRow partitionValue = dataSplit.partition();
                selectedPartitionValues.add(partitionValue);
                Optional<List<RawFile>> optRawFiles = dataSplit.convertToRawFiles();
                Optional<List<DeletionFile>> optDeletionFiles = dataSplit.deletionFiles();
                if (optRawFiles.isPresent()) {
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
                            Path finalDataFilePath = locationPath.toStorageLocation();
                            try {
                                List<Split> dorisSplits = splitFile(
                                        finalDataFilePath,
                                        0,
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
                        for (RawFile file : rawFiles) {
                            LocationPath locationPath = new LocationPath(file.path(),
                                    source.getCatalog().getProperties());
                            Path finalDataFilePath = locationPath.toStorageLocation();
                            try {
                                splits.addAll(
                                        splitFile(
                                                finalDataFilePath,
                                                0,
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
                } else {
                    splits.add(new PaimonSplit(split));
                    ++paimonSplitNum;
                }
            } else {
                splits.add(new PaimonSplit(split));
                ++paimonSplitNum;
            }
            splitStats.add(splitStat);
        }
        this.selectedPartitionNum = selectedPartitionValues.size();
        // TODO: get total partition number
        return splits;
    }

    private boolean supportNativeReader() {
        String fileFormat = source.getFileFormat().toLowerCase();
        switch (fileFormat) {
            case "orc":
            case "parquet":
                return true;
            default:
                return false;
        }
    }

    //When calling 'setPaimonParams' and 'getSplits', the column trimming has not been performed yet,
    // Therefore, paimon_column_names is temporarily reset here
    @Override
    public void updateRequiredSlots(PlanTranslatorContext planTranslatorContext,
            Set<SlotId> requiredByProjectSlotIdSet) throws UserException {
        super.updateRequiredSlots(planTranslatorContext, requiredByProjectSlotIdSet);
        String cols = desc.getSlots().stream().map(slot -> slot.getColumn().getName())
                .collect(Collectors.joining(","));
        for (TScanRangeLocations tScanRangeLocations : scanRangeLocations) {
            List<TFileRangeDesc> ranges = tScanRangeLocations.scan_range.ext_scan_range.file_scan_range.ranges;
            for (TFileRangeDesc tFileRangeDesc : ranges) {
                tFileRangeDesc.table_format_params.paimon_params.setPaimonColumnNames(cols);
            }
        }
    }

    @Override
    public TFileType getLocationType() throws DdlException, MetaNotFoundException {
        return getLocationType(((FileStoreTable) source.getPaimonTable()).location().toString());
    }

    @Override
    public TFileType getLocationType(String location) throws DdlException, MetaNotFoundException {
        return Optional.ofNullable(LocationPath.getTFileTypeForBE(location)).orElseThrow(() ->
                new DdlException("Unknown file location " + location + " for paimon table "));
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        return TFileFormatType.FORMAT_JNI;
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        //                return new ArrayList<>(source.getPaimonTable().partitionKeys());
        //Paymon is not aware of partitions and bypasses some existing logic by returning an empty list
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
        String result = super.getNodeExplainString(prefix, detailLevel)
                + String.format("%spaimonNativeReadSplits=%d/%d\n",
                        prefix, rawFileSplitNum, (paimonSplitNum + rawFileSplitNum));
        if (detailLevel == TExplainLevel.VERBOSE) {
            result += prefix + "PaimonSplitStats: \n";
            for (SplitStat splitStat : splitStats) {
                result += String.format("%s  %s\n", prefix, splitStat);
            }
        }
        return result;
    }
}
