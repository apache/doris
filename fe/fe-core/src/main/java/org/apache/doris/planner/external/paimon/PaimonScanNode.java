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

package org.apache.doris.planner.external.paimon;

import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.catalog.external.PaimonExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.external.FileQueryScanNode;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPaimonFileDesc;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TTableFormatFileDesc;

import avro.shaded.com.google.common.base.Preconditions;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.utils.InstantiationUtil;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PaimonScanNode extends FileQueryScanNode {
    private static PaimonSource source = null;
    private static List<Predicate> predicates;

    public PaimonScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "PAIMON_SCAN_NODE", StatisticalType.PAIMON_SCAN_NODE, needCheckColumnPriv);
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
        source = new PaimonSource((PaimonExternalTable) table, desc, columnNameToRange);
        Preconditions.checkNotNull(source);
        initSchemaParams();
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

    public static void setPaimonParams(TFileRangeDesc rangeDesc, PaimonSplit paimonSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(paimonSplit.getTableFormatType().value());
        TPaimonFileDesc fileDesc = new TPaimonFileDesc();
        fileDesc.setPaimonSplit(encodeObjectToString(paimonSplit.getSplit()));
        fileDesc.setPaimonPredicate(encodeObjectToString(predicates));
        fileDesc.setPaimonColumnNames(source.getDesc().getSlots().stream().map(slot -> slot.getColumn().getName())
                .collect(Collectors.joining(",")));
        fileDesc.setDbName(((PaimonExternalTable) source.getTargetTable()).getDbName());
        fileDesc.setPaimonOptions(((PaimonExternalCatalog) source.getCatalog()).getPaimonOptionsMap());
        fileDesc.setTableName(source.getTargetTable().getName());
        fileDesc.setCtlId(source.getCatalog().getId());
        fileDesc.setDbId(((PaimonExternalTable) source.getTargetTable()).getDbId());
        fileDesc.setTblId(source.getTargetTable().getId());
        fileDesc.setLastUpdateTime(source.getTargetTable().getLastUpdateTime());
        tableFormatFileDesc.setPaimonParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    public List<Split> getSplits() throws UserException {
        List<Split> splits = new ArrayList<>();
        int[] projected = desc.getSlots().stream().mapToInt(
                slot -> (source.getPaimonTable().rowType().getFieldNames().indexOf(slot.getColumn().getName())))
                .toArray();
        ReadBuilder readBuilder = source.getPaimonTable().newReadBuilder();
        List<org.apache.paimon.table.source.Split> paimonSplits = readBuilder.withFilter(predicates)
                .withProjection(projected)
                .newScan().plan().splits();
        for (org.apache.paimon.table.source.Split split : paimonSplits) {
            PaimonSplit paimonSplit = new PaimonSplit(split);
            splits.add(paimonSplit);
        }
        return splits;
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
        return getLocationType(((AbstractFileStoreTable) source.getPaimonTable()).location().toString());
    }

    @Override
    public TFileType getLocationType(String location) throws DdlException, MetaNotFoundException {
        //todo: no use
        return TFileType.FILE_S3;
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
    public TFileAttributes getFileAttributes() throws UserException {
        return source.getFileAttributes();
    }

    @Override
    public TableIf getTargetTable() {
        return source.getTargetTable();
    }

    @Override
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        return source.getCatalog().getCatalogProperty().getHadoopProperties();
    }

}
