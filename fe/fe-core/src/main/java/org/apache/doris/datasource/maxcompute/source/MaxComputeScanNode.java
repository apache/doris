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
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TMaxComputeFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.aliyun.odps.OdpsType;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MaxComputeScanNode extends FileQueryScanNode {

    private final MaxComputeExternalTable table;
    TableBatchReadSession tableBatchReadSession;

    public MaxComputeScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        this(id, desc, "MCScanNode", StatisticalType.MAX_COMPUTE_SCAN_NODE, needCheckColumnPriv);
    }

    public MaxComputeScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                              StatisticalType statisticalType, boolean needCheckColumnPriv) {
        super(id, desc, planNodeName, statisticalType, needCheckColumnPriv);
        table = (MaxComputeExternalTable) desc.getTable();
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
        tableFormatFileDesc.setMaxComputeParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
        rangeDesc.setPath("[ " + maxComputeSplit.getStart() + " , " + maxComputeSplit.getLength() + " ]");
        rangeDesc.setStartOffset(maxComputeSplit.getStart());
        rangeDesc.setSize(maxComputeSplit.getLength());
    }

    void createTableBatchReadSession() throws UserException {
        Predicate filterPredicate = convertPredicate();


        List<String> requiredPartitionColumns = new ArrayList<>();
        List<String> orderedRequiredDataColumns = new ArrayList<>();

        Set<String> requiredSlots =
                desc.getSlots().stream().map(e -> e.getColumn().getName()).collect(Collectors.toSet());

        Set<String> partitionColumns =
                table.getPartitionColumns().stream().map(Column::getName).collect(Collectors.toSet());

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



        MaxComputeExternalCatalog mcCatalog = (MaxComputeExternalCatalog) table.getCatalog();

        try {
            TableReadSessionBuilder scanBuilder = new TableReadSessionBuilder();
            tableBatchReadSession =
                    scanBuilder.identifier(TableIdentifier.of(table.getDbName(), table.getName()))
                            .withSettings(mcCatalog.getSettings())
                            .withSplitOptions(mcCatalog.getSplitOption())
                            .requiredPartitionColumns(requiredPartitionColumns)
                            .requiredDataColumns(orderedRequiredDataColumns)
                            .withArrowOptions(
                                    ArrowOptions.newBuilder()
                                            .withDatetimeUnit(TimestampUnit.MILLI)
                                            .withTimestampUnit(TimestampUnit.NANO)
                                            .build()
                            )
                            .withFilterPredicate(filterPredicate)
                            .buildBatchReadSession();
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }

    }

    protected Predicate convertPredicate() {
        if (conjuncts.isEmpty()) {
            return Predicate.NO_PREDICATE;
        }

        if (conjuncts.size() == 1) {
            try {
                return convertExprToOdpsPredicate(conjuncts.get(0));
            } catch (AnalysisException e) {
                Log.info("Failed to convert predicate " + conjuncts.get(0) + " to odps predicate");
                Log.info("Reason: " + e.getMessage());
                return Predicate.NO_PREDICATE;
            }
        }

        com.aliyun.odps.table.optimizer.predicate.CompoundPredicate
                filterPredicate = new com.aliyun.odps.table.optimizer.predicate.CompoundPredicate(
                        com.aliyun.odps.table.optimizer.predicate.CompoundPredicate.Operator.AND
        );

        for (Expr predicate : conjuncts) {
            try {
                filterPredicate.addPredicate(convertExprToOdpsPredicate(predicate));
            } catch (AnalysisException e) {
                Log.info("Failed to convert predicate " + predicate);
                Log.info("Reason: " + e.getMessage());
                return Predicate.NO_PREDICATE;
            }
        }
        return filterPredicate;
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
            if (inPredicate.getChildren().size() > 2) {
                return Predicate.NO_PREDICATE;
            }
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
                return  " \"" + dateLiteral.getStringValue(dstType) + "\" ";
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

    @Override
    public List<Split> getSplits() throws UserException {
        List<Split> result = new ArrayList<>();
        com.aliyun.odps.Table odpsTable = table.getOdpsTable();
        if (desc.getSlots().isEmpty() || odpsTable.getFileNum() <= 0) {
            return result;
        }
        createTableBatchReadSession();

        try {
            String scanSessionSerialize =  serializeSession(tableBatchReadSession);
            InputSplitAssigner assigner = tableBatchReadSession.getInputSplitAssigner();
            long modificationTime = table.getOdpsTable().getLastDataModifiedTime().getTime();

            MaxComputeExternalCatalog mcCatalog = (MaxComputeExternalCatalog) table.getCatalog();

            if (mcCatalog.getSplitStrategy().equals(MCProperties.SPLIT_BY_BYTE_SIZE_STRATEGY)) {

                for (com.aliyun.odps.table.read.split.InputSplit split : assigner.getAllSplits()) {
                    MaxComputeSplit maxComputeSplit =
                            new MaxComputeSplit(new LocationPath("/byte_size", Maps.newHashMap()),
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
                            new MaxComputeSplit(new LocationPath("/row_offset", Maps.newHashMap()),
                            offset, recordsPerSplit, totalRowCount, modificationTime, null,
                            Collections.emptyList());

                    maxComputeSplit.scanSerialize = scanSessionSerialize;
                    maxComputeSplit.splitType = SplitType.ROW_OFFSET;
                    maxComputeSplit.sessionId = split.getSessionId();

                    result.add(maxComputeSplit);
                }
            }
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
