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

package org.apache.doris.datasource.maxcompute.source;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.datasource.maxcompute.source.MaxComputeSplit.SplitType;
import org.apache.doris.datasource.property.constants.MCProperties;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TMaxComputeFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit;
import com.aliyun.odps.table.optimizer.predicate.Predicate;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import com.google.common.collect.Maps;
import jline.internal.Log;
import lombok.Setter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class MaxComputeScanNode extends FileQueryScanNode {

    private final MaxComputeExternalTable table;
    private Predicate filterPredicate;
    List<String> requiredPartitionColumns = new ArrayList<>();
    List<String> orderedRequiredDataColumns = new ArrayList<>();

    private int connectTimeout;
    private int readTimeout;
    private int retryTimes;

    @Setter
    private SelectedPartitions selectedPartitions = null;

    private static final LocationPath ROW_OFFSET_PATH = new LocationPath("/row_offset", Maps.newHashMap());
    private static final LocationPath BYTE_SIZE_PATH = new LocationPath("/byte_size", Maps.newHashMap());


    // For new planner
    public MaxComputeScanNode(PlanNodeId id, TupleDescriptor desc,
            SelectedPartitions selectedPartitions, boolean needCheckColumnPriv,
            SessionVariable sv) {
        this(id, desc, "MCScanNode", StatisticalType.MAX_COMPUTE_SCAN_NODE,
                selectedPartitions, needCheckColumnPriv, sv);
    }

    // For old planner
    public MaxComputeScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv,
            SessionVariable sv) {
        this(id, desc, "MCScanNode", StatisticalType.MAX_COMPUTE_SCAN_NODE,
                SelectedPartitions.NOT_PRUNED, needCheckColumnPriv, sv);
    }

    private MaxComputeScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
            StatisticalType statisticalType, SelectedPartitions selectedPartitions,
            boolean needCheckColumnPriv, SessionVariable sv) {
        super(id, desc, planNodeName, statisticalType, needCheckColumnPriv, sv);
        table = (MaxComputeExternalTable) desc.getTable();
        this.selectedPartitions = selectedPartitions;
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof MaxComputeSplit) {
            setScanParams(rangeDesc, (MaxComputeSplit) split);
        }
    }

    private void setScanParams(TFileRangeDesc rangeDesc, MaxComputeSplit maxComputeSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(TableFormatType.MAX_COMPUTE.value());
        TMaxComputeFileDesc fileDesc = new TMaxComputeFileDesc();
        fileDesc.setPartitionSpec("deprecated");
        fileDesc.setTableBatchReadSession(maxComputeSplit.scanSerialize);
        fileDesc.setSessionId(maxComputeSplit.getSessionId());

        fileDesc.setReadTimeout(readTimeout);
        fileDesc.setConnectTimeout(connectTimeout);
        fileDesc.setRetryTimes(retryTimes);

        tableFormatFileDesc.setMaxComputeParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
        rangeDesc.setPath("[ " + maxComputeSplit.getStart() + " , " + maxComputeSplit.getLength() + " ]");
        rangeDesc.setStartOffset(maxComputeSplit.getStart());
        rangeDesc.setSize(maxComputeSplit.getLength());
    }


    private void createRequiredColumns() {
        Set<String> requiredSlots =
                desc.getSlots().stream().map(e -> e.getColumn().getName()).collect(Collectors.toSet());

        Set<String> partitionColumns =
                table.getPartitionColumns().stream().map(Column::getName).collect(Collectors.toSet());

        requiredPartitionColumns.clear();
        orderedRequiredDataColumns.clear();

        for (Column column : table.getColumns()) {
            String columnName =  column.getName();
            if (!requiredSlots.contains(columnName)) {
                continue;
            }
            if (partitionColumns.contains(columnName)) {
                requiredPartitionColumns.add(columnName);
            } else {
                orderedRequiredDataColumns.add(columnName);
            }
        }
    }

    /**
     * For no partition table: request requiredPartitionSpecs is empty
     * For partition table: if requiredPartitionSpecs is empty, get all partition data.
     */
    TableBatchReadSession createTableBatchReadSession(List<PartitionSpec> requiredPartitionSpecs) throws IOException {
        MaxComputeExternalCatalog mcCatalog = (MaxComputeExternalCatalog) table.getCatalog();

        readTimeout = mcCatalog.getReadTimeout();
        connectTimeout = mcCatalog.getConnectTimeout();
        retryTimes = mcCatalog.getRetryTimes();

        TableReadSessionBuilder scanBuilder = new TableReadSessionBuilder();
        return scanBuilder.identifier(TableIdentifier.of(table.getDbName(), table.getName()))
                        .withSettings(mcCatalog.getSettings())
                        .withSplitOptions(mcCatalog.getSplitOption())
                        .requiredPartitionColumns(requiredPartitionColumns)
                        .requiredDataColumns(orderedRequiredDataColumns)
                        .withFilterPredicate(filterPredicate)
                        .requiredPartitions(requiredPartitionSpecs)
                        .withArrowOptions(
                                ArrowOptions.newBuilder()
                                        .withDatetimeUnit(TimestampUnit.MILLI)
                                        .withTimestampUnit(TimestampUnit.NANO)
                                        .build()
                        ).buildBatchReadSession();
    }

    @Override
    public boolean isBatchMode() {
        if (table.getPartitionColumns().isEmpty()) {
            return false;
        }

        com.aliyun.odps.Table odpsTable = table.getOdpsTable();
        if (desc.getSlots().isEmpty() || odpsTable.getFileNum() <= 0) {
            return false;
        }

        int numPartitions = sessionVariable.getNumPartitionsInBatchMode();
        return numPartitions > 0
                && selectedPartitions != SelectedPartitions.NOT_PRUNED
                && selectedPartitions.selectedPartitions.size() >= numPartitions;
    }

    @Override
    public int numApproximateSplits() {
        return selectedPartitions.selectedPartitions.size();
    }

    @Override
    public void startSplit(int numBackends) {
        this.totalPartitionNum = selectedPartitions.totalPartitionNum;
        this.selectedPartitionNum = selectedPartitions.selectedPartitions.size();

        if (selectedPartitions.selectedPartitions.isEmpty()) {
            //no need read any partition data.
            return;
        }

        createRequiredColumns();
        List<PartitionSpec> requiredPartitionSpecs = new ArrayList<>();
        selectedPartitions.selectedPartitions.forEach(
                (key, value) -> requiredPartitionSpecs.add(new PartitionSpec(key))
        );

        int batchNumPartitions = sessionVariable.getNumPartitionsInBatchMode();

        Executor scheduleExecutor = Env.getCurrentEnv().getExtMetaCacheMgr().getScheduleExecutor();
        AtomicReference<UserException> batchException = new AtomicReference<>(null);
        AtomicInteger numFinishedPartitions = new AtomicInteger(0);

        CompletableFuture.runAsync(() -> {
            for (int beginIndex = 0; beginIndex < requiredPartitionSpecs.size(); beginIndex += batchNumPartitions) {
                int endIndex = Math.min(beginIndex + batchNumPartitions, requiredPartitionSpecs.size());
                if (batchException.get() != null || splitAssignment.isStop()) {
                    break;
                }
                List<PartitionSpec> requiredBatchPartitionSpecs = requiredPartitionSpecs.subList(beginIndex, endIndex);
                int curBatchSize = endIndex - beginIndex;

                try {
                    CompletableFuture.runAsync(() -> {
                        try {
                            TableBatchReadSession tableBatchReadSession =
                                    createTableBatchReadSession(requiredBatchPartitionSpecs);
                            List<Split> batchSplit = getSplitByTableSession(tableBatchReadSession);

                            splitAssignment.addToQueue(batchSplit);
                        } catch (IOException e) {
                            batchException.set(new UserException(e.getMessage(), e));
                        } finally {
                            if (batchException.get() != null) {
                                splitAssignment.setException(batchException.get());
                            }

                            if (numFinishedPartitions.addAndGet(curBatchSize) == requiredPartitionSpecs.size()) {
                                splitAssignment.finishSchedule();
                            }
                        }
                    }, scheduleExecutor);
                } catch (Exception e) {
                    batchException.set(new UserException(e.getMessage(), e));
                }

                if (batchException.get() != null) {
                    splitAssignment.setException(batchException.get());
                }
            }
        });
    }

    @Override
    protected void convertPredicate() {
        if (conjuncts.isEmpty()) {
            this.filterPredicate = Predicate.NO_PREDICATE;
        }

        List<Predicate> odpsPredicates = new ArrayList<>();
        for (Expr dorisPredicate : conjuncts) {
            try {
                odpsPredicates.add(convertExprToOdpsPredicate(dorisPredicate));
            } catch (AnalysisException e) {
                Log.warn("Failed to convert predicate " + dorisPredicate.toString() + "Reason: "
                        + e.getMessage());
            }
        }

        if (odpsPredicates.isEmpty()) {
            this.filterPredicate = Predicate.NO_PREDICATE;
        } else if (odpsPredicates.size() == 1) {
            this.filterPredicate = odpsPredicates.get(0);
        } else {
            com.aliyun.odps.table.optimizer.predicate.CompoundPredicate
                    filterPredicate = new com.aliyun.odps.table.optimizer.predicate.CompoundPredicate(
                    com.aliyun.odps.table.optimizer.predicate.CompoundPredicate.Operator.AND);

            for (Predicate odpsPredicate : odpsPredicates) {
                filterPredicate.addPredicate(odpsPredicate);
            }
            this.filterPredicate = filterPredicate;
        }
    }

    private Predicate convertExprToOdpsPredicate(Expr expr) throws AnalysisException {
        Predicate odpsPredicate = null;
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) expr;

            com.aliyun.odps.table.optimizer.predicate.CompoundPredicate.Operator odpsOp;
            switch (compoundPredicate.getOp()) {
                case AND:
                    odpsOp = com.aliyun.odps.table.optimizer.predicate.CompoundPredicate.Operator.AND;
                    break;
                case OR:
                    odpsOp = com.aliyun.odps.table.optimizer.predicate.CompoundPredicate.Operator.OR;
                    break;
                case NOT:
                    odpsOp = com.aliyun.odps.table.optimizer.predicate.CompoundPredicate.Operator.NOT;
                    break;
                default:
                    throw new AnalysisException("Unknown operator: " + compoundPredicate.getOp());
            }

            List<Predicate> odpsPredicates = new ArrayList<>();

            odpsPredicates.add(convertExprToOdpsPredicate(expr.getChild(0)));

            if (compoundPredicate.getOp() != Operator.NOT) {
                odpsPredicates.add(convertExprToOdpsPredicate(expr.getChild(1)));
            }
            odpsPredicate = new com.aliyun.odps.table.optimizer.predicate.CompoundPredicate(odpsOp, odpsPredicates);

        } else if (expr instanceof InPredicate) {

            InPredicate inPredicate = (InPredicate) expr;
            com.aliyun.odps.table.optimizer.predicate.InPredicate.Operator odpsOp =
                    inPredicate.isNotIn()
                            ? com.aliyun.odps.table.optimizer.predicate.InPredicate.Operator.IN
                            : com.aliyun.odps.table.optimizer.predicate.InPredicate.Operator.NOT_IN;

            String columnName = convertSlotRefToColumnName(expr.getChild(0));
            com.aliyun.odps.OdpsType odpsType  =  table.getColumnNameToOdpsColumn().get(columnName).getType();

            StringBuilder stringBuilder = new StringBuilder();


            stringBuilder.append(columnName);
            stringBuilder.append(" ");
            stringBuilder.append(odpsOp.getDescription());
            stringBuilder.append(" (");

            for (int i = 1; i < inPredicate.getChildren().size(); i++) {
                stringBuilder.append(convertLiteralToOdpsValues(odpsType, expr.getChild(i)));
                if (i < inPredicate.getChildren().size() - 1) {
                    stringBuilder.append(", ");
                }
            }
            stringBuilder.append(" )");

            odpsPredicate = new com.aliyun.odps.table.optimizer.predicate.RawPredicate(stringBuilder.toString());

        } else if (expr instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) expr;


            com.aliyun.odps.table.optimizer.predicate.BinaryPredicate.Operator odpsOp;
            switch (binaryPredicate.getOp()) {
                case EQ: {
                    odpsOp = com.aliyun.odps.table.optimizer.predicate.BinaryPredicate.Operator.EQUALS;
                    break;
                }
                case NE: {
                    odpsOp = com.aliyun.odps.table.optimizer.predicate.BinaryPredicate.Operator.NOT_EQUALS;
                    break;
                }
                case GE: {
                    odpsOp = com.aliyun.odps.table.optimizer.predicate.BinaryPredicate.Operator.GREATER_THAN_OR_EQUAL;
                    break;
                }
                case LE: {
                    odpsOp = com.aliyun.odps.table.optimizer.predicate.BinaryPredicate.Operator.LESS_THAN_OR_EQUAL;
                    break;
                }
                case LT: {
                    odpsOp = com.aliyun.odps.table.optimizer.predicate.BinaryPredicate.Operator.LESS_THAN;
                    break;
                }
                case GT: {
                    odpsOp = com.aliyun.odps.table.optimizer.predicate.BinaryPredicate.Operator.GREATER_THAN;
                    break;
                }
                default: {
                    odpsOp = null;
                    break;
                }
            }

            if (odpsOp != null) {
                String columnName = convertSlotRefToColumnName(expr.getChild(0));
                com.aliyun.odps.OdpsType odpsType  =  table.getColumnNameToOdpsColumn().get(columnName).getType();
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(columnName);
                stringBuilder.append(" ");
                stringBuilder.append(odpsOp.getDescription());
                stringBuilder.append(" ");
                stringBuilder.append(convertLiteralToOdpsValues(odpsType, expr.getChild(1)));

                odpsPredicate = new com.aliyun.odps.table.optimizer.predicate.RawPredicate(stringBuilder.toString());
            }
        } else if (expr instanceof IsNullPredicate) {
            IsNullPredicate isNullPredicate = (IsNullPredicate) expr;
            com.aliyun.odps.table.optimizer.predicate.UnaryPredicate.Operator odpsOp =
                    isNullPredicate.isNotNull()
                            ? com.aliyun.odps.table.optimizer.predicate.UnaryPredicate.Operator.NOT_NULL
                            : com.aliyun.odps.table.optimizer.predicate.UnaryPredicate.Operator.IS_NULL;

            odpsPredicate =  new com.aliyun.odps.table.optimizer.predicate.UnaryPredicate(odpsOp,
                    new com.aliyun.odps.table.optimizer.predicate.Attribute(
                        convertSlotRefToColumnName(expr.getChild(0))
                    )
            );
        }


        if (odpsPredicate == null) {
            throw new AnalysisException("Do not support convert ["
                    + expr.getExprName() + "] in convertExprToOdpsPredicate.");
        }
        return odpsPredicate;
    }

    private String convertSlotRefToColumnName(Expr expr) throws AnalysisException {
        if (expr instanceof SlotRef) {
            return ((SlotRef) expr).getColumnName();
        } else if (expr instanceof CastExpr) {
            if (expr.getChild(0) instanceof SlotRef) {
                return ((SlotRef) expr.getChild(0)).getColumnName();
            }
        }

        throw new AnalysisException("Do not support convert ["
                + expr.getExprName() + "] in convertSlotRefToAttribute.");

    }

    private String convertLiteralToOdpsValues(OdpsType odpsType, Expr expr) throws AnalysisException {
        if (!(expr instanceof LiteralExpr)) {
            throw new AnalysisException("Do not support convert ["
                    + expr.getExprName() + "] in convertSlotRefToAttribute.");
        }
        LiteralExpr literalExpr = (LiteralExpr) expr;

        switch (odpsType) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case DECIMAL:
            case FLOAT:
            case DOUBLE: {
                return " " + literalExpr.toString() + " ";
            }
            case STRING:
            case CHAR:
            case VARCHAR: {
                return " \"" + literalExpr.toString() + "\" ";
            }
            case DATE: {
                DateLiteral dateLiteral = (DateLiteral) literalExpr;
                ScalarType dstType = ScalarType.createDateV2Type();
                return  " \"" + dateLiteral.getStringValue(dstType) + "\" ";
            }
            case DATETIME: {
                DateLiteral dateLiteral = (DateLiteral) literalExpr;
                ScalarType dstType = ScalarType.createDatetimeV2Type(3);

                return  " \"" + convertDateTimezone(dateLiteral.getStringValue(dstType),
                                    ((MaxComputeExternalCatalog) table.getCatalog()).getProjectDateTimeZone()) + "\" ";
            }
            case TIMESTAMP_NTZ: {
                DateLiteral dateLiteral = (DateLiteral) literalExpr;
                ScalarType dstType = ScalarType.createDatetimeV2Type(6);
                return  " \"" + dateLiteral.getStringValue(dstType) + "\" ";
            }
            default: {
                break;
            }
        }
        throw new AnalysisException("Do not support convert odps type [" + odpsType + "] to odps values.");
    }


    public static String convertDateTimezone(String dateTimeStr, ZoneId toZone) {
        if (DateUtils.getTimeZone().equals(toZone)) {
            return dateTimeStr;
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        LocalDateTime localDateTime = LocalDateTime.parse(dateTimeStr, formatter);

        ZonedDateTime sourceZonedDateTime = localDateTime.atZone(DateUtils.getTimeZone());
        ZonedDateTime targetZonedDateTime = sourceZonedDateTime.withZoneSameInstant(toZone);

        return targetZonedDateTime.format(formatter);
    }



    @Override
    public TFileFormatType getFileFormatType() {
        return TFileFormatType.FORMAT_JNI;
    }

    @Override
    public List<String> getPathPartitionKeys() {
        return Collections.emptyList();
    }

    @Override
    protected TableIf getTargetTable() throws UserException {
        return table;
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        return new HashMap<>();
    }

    private List<Split> getSplitByTableSession(TableBatchReadSession tableBatchReadSession) throws IOException {
        List<Split> result = new ArrayList<>();
        String scanSessionSerialize =  serializeSession(tableBatchReadSession);
        InputSplitAssigner assigner = tableBatchReadSession.getInputSplitAssigner();
        long modificationTime = table.getOdpsTable().getLastDataModifiedTime().getTime();

        MaxComputeExternalCatalog mcCatalog = (MaxComputeExternalCatalog) table.getCatalog();

        if (mcCatalog.getSplitStrategy().equals(MCProperties.SPLIT_BY_BYTE_SIZE_STRATEGY)) {

            for (com.aliyun.odps.table.read.split.InputSplit split : assigner.getAllSplits()) {
                MaxComputeSplit maxComputeSplit =
                        new MaxComputeSplit(BYTE_SIZE_PATH,
                                ((IndexedInputSplit) split).getSplitIndex(), -1,
                                mcCatalog.getSplitByteSize(),
                                modificationTime, null,
                                Collections.emptyList());


                maxComputeSplit.scanSerialize = scanSessionSerialize;
                maxComputeSplit.splitType = SplitType.BYTE_SIZE;
                maxComputeSplit.sessionId = split.getSessionId();

                result.add(maxComputeSplit);
            }
        } else {
            long totalRowCount =  assigner.getTotalRowCount();

            long recordsPerSplit = mcCatalog.getSplitRowCount();
            for (long offset = 0; offset < totalRowCount; offset += recordsPerSplit) {
                recordsPerSplit = Math.min(recordsPerSplit, totalRowCount - offset);
                com.aliyun.odps.table.read.split.InputSplit split =
                        assigner.getSplitByRowOffset(offset, recordsPerSplit);

                MaxComputeSplit maxComputeSplit =
                        new MaxComputeSplit(ROW_OFFSET_PATH,
                                offset, recordsPerSplit, totalRowCount, modificationTime, null,
                                Collections.emptyList());

                maxComputeSplit.scanSerialize = scanSessionSerialize;
                maxComputeSplit.splitType = SplitType.ROW_OFFSET;
                maxComputeSplit.sessionId = split.getSessionId();

                result.add(maxComputeSplit);
            }
        }
        return result;
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        List<Split> result = new ArrayList<>();
        com.aliyun.odps.Table odpsTable = table.getOdpsTable();
        if (desc.getSlots().isEmpty() || odpsTable.getFileNum() <= 0) {
            return result;
        }

        createRequiredColumns();

        List<PartitionSpec> requiredPartitionSpecs = new ArrayList<>();
        //if requiredPartitionSpecs is empty, get all partition data.
        if (!table.getPartitionColumns().isEmpty() && selectedPartitions != SelectedPartitions.NOT_PRUNED) {
            this.totalPartitionNum = selectedPartitions.totalPartitionNum;
            this.selectedPartitionNum = selectedPartitions.selectedPartitions.size();

            if (selectedPartitions.selectedPartitions.isEmpty()) {
                //no need read any partition data.
                return result;
            }
            selectedPartitions.selectedPartitions.forEach(
                    (key, value) -> requiredPartitionSpecs.add(new PartitionSpec(key))
            );
        }

        try {
            TableBatchReadSession tableBatchReadSession = createTableBatchReadSession(requiredPartitionSpecs);
            result = getSplitByTableSession(tableBatchReadSession);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private static String serializeSession(Serializable object) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(object);
        byte[] serializedBytes = byteArrayOutputStream.toByteArray();
        return Base64.getEncoder().encodeToString(serializedBytes);
    }
}
