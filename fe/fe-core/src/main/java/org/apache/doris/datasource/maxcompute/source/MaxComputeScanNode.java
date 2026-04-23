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
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToExprNameVisitor;
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
import org.apache.doris.common.maxcompute.MCProperties;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.datasource.maxcompute.source.MaxComputeSplit.SplitType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TMaxComputeFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.optimizer.predicate.Predicate;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import jline.internal.Log;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    static final DateTimeFormatter dateTime3Formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    static final DateTimeFormatter dateTime6Formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private static final Logger LOG = LogManager.getLogger(MaxComputeScanNode.class);

    private final MaxComputeExternalTable table;
    private Predicate filterPredicate;
    List<String> requiredPartitionColumns = new ArrayList<>();
    List<String> orderedRequiredDataColumns = new ArrayList<>();

    private int connectTimeout;
    private int readTimeout;
    private int retryTimes;

    private boolean onlyPartitionEqualityPredicate = false;

    @Setter
    private SelectedPartitions selectedPartitions = null;

    private static final LocationPath ROW_OFFSET_PATH = LocationPath.of("/row_offset");
    private static final LocationPath BYTE_SIZE_PATH = LocationPath.of("/byte_size");


    // For new planner
    public MaxComputeScanNode(PlanNodeId id, TupleDescriptor desc,
            SelectedPartitions selectedPartitions, boolean needCheckColumnPriv,
            SessionVariable sv, ScanContext scanContext) {
        this(id, desc, "MCScanNode", selectedPartitions, needCheckColumnPriv, sv, scanContext);
    }

    private MaxComputeScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
            SelectedPartitions selectedPartitions, boolean needCheckColumnPriv, SessionVariable sv,
            ScanContext scanContext) {
        super(id, desc, planNodeName, scanContext, needCheckColumnPriv, sv);
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
        return createTableBatchReadSession(requiredPartitionSpecs, mcCatalog.getSplitOption());
    }

    TableBatchReadSession createTableBatchReadSession(
            List<PartitionSpec> requiredPartitionSpecs, SplitOptions splitOptions) throws IOException {
        MaxComputeExternalCatalog mcCatalog = (MaxComputeExternalCatalog) table.getCatalog();

        readTimeout = mcCatalog.getReadTimeout();
        connectTimeout = mcCatalog.getConnectTimeout();
        retryTimes = mcCatalog.getRetryTimes();

        TableReadSessionBuilder scanBuilder = new TableReadSessionBuilder();

        return scanBuilder.identifier(table.getTableIdentifier())
                        .withSettings(mcCatalog.getSettings())
                .withSplitOptions(splitOptions)
                        .requiredPartitionColumns(requiredPartitionColumns)
                        .requiredDataColumns(orderedRequiredDataColumns)
                        .withFilterPredicate(filterPredicate)
                        .requiredPartitions(requiredPartitionSpecs)
                        .withArrowOptions(
                                ArrowOptions.newBuilder()
                                        .withDatetimeUnit(TimestampUnit.MILLI)
                                        .withTimestampUnit(TimestampUnit.MICRO)
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

                            if (splitAssignment.needMoreSplit()) {
                                splitAssignment.addToQueue(batchSplit);
                            }
                        } catch (Exception e) {
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
        }, scheduleExecutor);
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
            } catch (Exception e) {
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

        this.onlyPartitionEqualityPredicate = checkOnlyPartitionEqualityPredicate();
    }

    private boolean checkOnlyPartitionEqualityPredicate() {
        if (conjuncts.isEmpty()) {
            return true;
        }
        Set<String> partitionColumns =
                table.getPartitionColumns().stream().map(Column::getName).collect(Collectors.toSet());
        for (Expr expr : conjuncts) {
            if (expr instanceof BinaryPredicate) {
                BinaryPredicate bp = (BinaryPredicate) expr;
                if (bp.getOp() != BinaryPredicate.Operator.EQ) {
                    return false;
                }
                if (!(bp.getChild(0) instanceof SlotRef) || !(bp.getChild(1) instanceof LiteralExpr)) {
                    return false;
                }
                String colName = ((SlotRef) bp.getChild(0)).getColumnName();
                if (!partitionColumns.contains(colName)) {
                    return false;
                }
            } else if (expr instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) expr;
                if (inPredicate.isNotIn()) {
                    return false;
                }
                if (!(inPredicate.getChild(0) instanceof SlotRef)) {
                    return false;
                }
                String colName = ((SlotRef) inPredicate.getChild(0)).getColumnName();
                if (!partitionColumns.contains(colName)) {
                    return false;
                }
                for (int i = 1; i < inPredicate.getChildren().size(); i++) {
                    if (!(inPredicate.getChild(i) instanceof LiteralExpr)) {
                        return false;
                    }
                }
            } else {
                return false;
            }
        }
        return true;
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
            if (!table.getColumnNameToOdpsColumn().containsKey(columnName)) {
                Map<String, com.aliyun.odps.Column> columnMap = table.getColumnNameToOdpsColumn();
                LOG.warn("ColumnNameToOdpsColumn size=" + columnMap.size()
                        + ", keys=[" + String.join(", ", columnMap.keySet()) + "]");
                throw new AnalysisException("Column " + columnName + " not found in table, can not push "
                        + "down predicate to MaxCompute " + table.getName());
            }
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
                if (!table.getColumnNameToOdpsColumn().containsKey(columnName)) {
                    Map<String, com.aliyun.odps.Column> columnMap = table.getColumnNameToOdpsColumn();
                    LOG.warn("ColumnNameToOdpsColumn size=" + columnMap.size()
                            + ", keys=[" + String.join(", ", columnMap.keySet()) + "]");
                    throw new AnalysisException("Column " + columnName + " not found in table, can not push "
                            + "down predicate to MaxCompute " + table.getName());
                }
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
                    + expr.accept(ExprToExprNameVisitor.INSTANCE, null)
                    + "] in convertExprToOdpsPredicate.");
        }
        return odpsPredicate;
    }

    private String convertSlotRefToColumnName(Expr expr) throws AnalysisException {
        if (expr instanceof SlotRef) {
            return ((SlotRef) expr).getColumnName();
        }

        throw new AnalysisException("Do not support convert ["
                + expr.accept(ExprToExprNameVisitor.INSTANCE, null)
                + "] in convertSlotRefToAttribute.");

    }

    private String convertLiteralToOdpsValues(OdpsType odpsType, Expr expr) throws AnalysisException {
        if (!(expr instanceof LiteralExpr)) {
            throw new AnalysisException("Do not support convert ["
                    + expr.accept(ExprToExprNameVisitor.INSTANCE, null)
                    + "] in convertSlotRefToAttribute.");
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
                MaxComputeExternalCatalog  mcCatalog = (MaxComputeExternalCatalog) table.getCatalog();
                if (mcCatalog.getDateTimePredicatePushDown()) {
                    DateLiteral dateLiteral = (DateLiteral) literalExpr;
                    ScalarType dstType = ScalarType.createDatetimeV2Type(3);

                    return " \"" + convertDateTimezone(dateLiteral.getStringValue(dstType), dateTime3Formatter,
                            ZoneId.of("UTC")) + "\" ";
                }
                break;
            }
            /**
             * Disable the predicate pushdown to the odps API because the timestamp precision of odps is 9 and the
             * mapping precision of Doris is 6. If we insert `2023-02-02 00:00:00.123456789` into odps, doris reads
             * it as `2023-02-02 00:00:00.123456`. Since "789" is missing, we cannot push it down correctly.
             */
            case TIMESTAMP: {
                MaxComputeExternalCatalog  mcCatalog = (MaxComputeExternalCatalog) table.getCatalog();
                if (mcCatalog.getDateTimePredicatePushDown()) {
                    DateLiteral dateLiteral = (DateLiteral) literalExpr;
                    ScalarType dstType = ScalarType.createDatetimeV2Type(6);

                    return  " \"" + convertDateTimezone(dateLiteral.getStringValue(dstType), dateTime6Formatter,
                            ZoneId.of("UTC")) + "\" ";
                }
                break;
            }
            case TIMESTAMP_NTZ: {
                MaxComputeExternalCatalog  mcCatalog = (MaxComputeExternalCatalog) table.getCatalog();
                if (mcCatalog.getDateTimePredicatePushDown()) {
                    DateLiteral dateLiteral = (DateLiteral) literalExpr;
                    ScalarType dstType = ScalarType.createDatetimeV2Type(6);
                    return " \"" + dateLiteral.getStringValue(dstType) + "\" ";
                }
                break;
            }
            default: {
                break;
            }
        }
        throw new AnalysisException("Do not support convert odps type [" + odpsType + "] to odps values.");
    }


    public static String convertDateTimezone(String dateTimeStr, DateTimeFormatter formatter, ZoneId toZone) {
        if (DateUtils.getTimeZone().equals(toZone)) {
            return dateTimeStr;
        }

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

        long t0 = System.currentTimeMillis();
        String scanSessionSerialize =  serializeSession(tableBatchReadSession);
        long t1 = System.currentTimeMillis();
        LOG.info("MaxComputeScanNode getSplitByTableSession: serializeSession cost {} ms, "
                + "serialized size: {} bytes", t1 - t0, scanSessionSerialize.length());

        InputSplitAssigner assigner = tableBatchReadSession.getInputSplitAssigner();
        long t2 = System.currentTimeMillis();
        LOG.info("MaxComputeScanNode getSplitByTableSession: getInputSplitAssigner cost {} ms", t2 - t1);

        long modificationTime = table.getOdpsTable().getLastDataModifiedTime().getTime();

        MaxComputeExternalCatalog mcCatalog = (MaxComputeExternalCatalog) table.getCatalog();

        if (mcCatalog.getSplitStrategy().equals(MCProperties.SPLIT_BY_BYTE_SIZE_STRATEGY)) {
            long t3 = System.currentTimeMillis();
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
            LOG.info("MaxComputeScanNode getSplitByTableSession: byte_size getAllSplits+build cost {} ms, "
                    + "splits size: {}", System.currentTimeMillis() - t3, result.size());
        } else {
            long t3 = System.currentTimeMillis();
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
            LOG.info("MaxComputeScanNode getSplitByTableSession: row_offset getSplitByRowOffset+build cost {} ms, "
                            + "splits size: {}, totalRowCount: {}", System.currentTimeMillis() - t3, result.size(),
                    totalRowCount);
        }

        return result;
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        long startTime = System.currentTimeMillis();
        List<Split> result = new ArrayList<>();
        com.aliyun.odps.Table odpsTable = table.getOdpsTable();
        long getOdpsTableTime = System.currentTimeMillis();
        LOG.info("MaxComputeScanNode getSplits: getOdpsTable cost {} ms", getOdpsTableTime - startTime);

        if (desc.getSlots().isEmpty() || odpsTable.getFileNum() <= 0) {
            return result;
        }
        long getFileNumTime = System.currentTimeMillis();
        LOG.info("MaxComputeScanNode getSplits: getFileNum cost {} ms", getFileNumTime - getOdpsTableTime);

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
            long beforeSession = System.currentTimeMillis();
            if (sessionVariable.enableMcLimitSplitOptimization
                    && onlyPartitionEqualityPredicate && hasLimit()) {
                result = getSplitsWithLimitOptimization(requiredPartitionSpecs);
            } else {
                TableBatchReadSession tableBatchReadSession = createTableBatchReadSession(requiredPartitionSpecs);
                long afterSession = System.currentTimeMillis();
                LOG.info("MaxComputeScanNode getSplits: createTableBatchReadSession cost {} ms, "
                        + "partitionSpecs size: {}", afterSession - beforeSession, requiredPartitionSpecs.size());

                result = getSplitByTableSession(tableBatchReadSession);
                long afterSplit = System.currentTimeMillis();
                LOG.info("MaxComputeScanNode getSplits: getSplitByTableSession cost {} ms, "
                        + "splits size: {}", afterSplit - afterSession, result.size());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        LOG.info("MaxComputeScanNode getSplits: total cost {} ms", System.currentTimeMillis() - startTime);
        return result;
    }

    private List<Split> getSplitsWithLimitOptimization(
            List<PartitionSpec> requiredPartitionSpecs) throws IOException {
        long startTime = System.currentTimeMillis();

        SplitOptions rowOffsetOptions = SplitOptions.newBuilder()
                .SplitByRowOffset()
                .withCrossPartition(false)
                .build();

        TableBatchReadSession tableBatchReadSession =
                createTableBatchReadSession(requiredPartitionSpecs, rowOffsetOptions);
        long afterSession = System.currentTimeMillis();
        LOG.info("MaxComputeScanNode getSplitsWithLimitOptimization: "
                + "createTableBatchReadSession cost {} ms", afterSession - startTime);

        String scanSessionSerialize = serializeSession(tableBatchReadSession);
        InputSplitAssigner assigner = tableBatchReadSession.getInputSplitAssigner();
        long totalRowCount = assigner.getTotalRowCount();

        LOG.info("MaxComputeScanNode getSplitsWithLimitOptimization: "
                + "totalRowCount={}, limit={}", totalRowCount, getLimit());

        List<Split> result = new ArrayList<>();
        if (totalRowCount <= 0) {
            return result;
        }

        long rowsToRead = Math.min(getLimit(), totalRowCount);
        long modificationTime = table.getOdpsTable().getLastDataModifiedTime().getTime();
        com.aliyun.odps.table.read.split.InputSplit split =
                assigner.getSplitByRowOffset(0, rowsToRead);

        MaxComputeSplit maxComputeSplit = new MaxComputeSplit(
                ROW_OFFSET_PATH, 0, rowsToRead, totalRowCount,
                modificationTime, null, Collections.emptyList());
        maxComputeSplit.scanSerialize = scanSessionSerialize;
        maxComputeSplit.splitType = SplitType.ROW_OFFSET;
        maxComputeSplit.sessionId = split.getSessionId();
        result.add(maxComputeSplit);

        LOG.info("MaxComputeScanNode getSplitsWithLimitOptimization: "
                        + "total cost {} ms, 1 split with {} rows",
                System.currentTimeMillis() - startTime, rowsToRead);
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
