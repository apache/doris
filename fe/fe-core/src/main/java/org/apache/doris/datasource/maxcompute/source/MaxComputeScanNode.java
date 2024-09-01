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
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.datasource.maxcompute.source.MaxComputeSplit.SplitType;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TMaxComputeFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit;
import com.aliyun.odps.table.configuration.SplitOptions;
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
    private static final int MIN_SPLIT_SIZE = 4096;
    String splitStrategy;
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
        rangeDesc.setPath("Not use");
        rangeDesc.setStartOffset(maxComputeSplit.getStart());
        rangeDesc.setSize(maxComputeSplit.getLength());
    }

    protected void doInitialize() throws UserException {
        super.doInitialize();
        Predicate filterPredicate = convertPredicate();


        List<String> requiredPartitionColumns = new ArrayList<>();
        List<String> requiredDataColumns = new ArrayList<>();


        ArrayList<SlotDescriptor> slots = desc.getSlots();
        Set<String> partitionColumns =
                table.getPartitionColumns().stream().map(col -> col.getName()).collect(Collectors.toSet());

        Set<String> columnNames =
                table.getColumns().stream().map(col -> col.getName()).collect(Collectors.toSet());
        for (SlotDescriptor slot  : slots) {
            String slotName = slot.getColumn().getName();
            if (partitionColumns.contains(slotName)) {
                requiredPartitionColumns.add(slotName);
            } else if (columnNames.contains(slotName)) {
                requiredDataColumns.add(slotName);
            }
        }

        splitStrategy = table.getCatalog().getCatalogProperty()
                .getOrDefault("split_strategy", "byte_size");

        TableReadSessionBuilder scanBuilder = new TableReadSessionBuilder();

        try {
            if (splitStrategy.equals("byte_size")) {
                tableBatchReadSession =
                        scanBuilder.identifier(TableIdentifier.of(table.getDbName(), table.getName()))
                                .withSettings(((MaxComputeExternalCatalog) table.getCatalog()).settings)
                                .withSplitOptions(SplitOptions.newBuilder()
                                        .SplitByByteSize(256 * 1024L * 1024L)
                                        .withCrossPartition(false).build())
                                .requiredPartitionColumns(requiredPartitionColumns)
                                .requiredDataColumns(requiredDataColumns)
                                .withArrowOptions(
                                        ArrowOptions.newBuilder()
                                                .withDatetimeUnit(TimestampUnit.MILLI)
                                                .withTimestampUnit(TimestampUnit.NANO)
                                                .build()
                                )
                                .withFilterPredicate(filterPredicate)
                                .buildBatchReadSession();
            } else {
                tableBatchReadSession =
                        scanBuilder.identifier(TableIdentifier.of(table.getDbName(), table.getName()))
                                .withSettings(((MaxComputeExternalCatalog) table.getCatalog()).settings)
                                .requiredPartitionColumns(requiredPartitionColumns)
                                .requiredDataColumns(requiredDataColumns)
                                .withSplitOptions(SplitOptions.newBuilder()
                                        .SplitByRowOffset()
                                        .withCrossPartition(false)
                                        .build())
                                .withArrowOptions(
                                        ArrowOptions.newBuilder()
                                                .withDatetimeUnit(TimestampUnit.MILLI)
                                                .withTimestampUnit(TimestampUnit.NANO)
                                                .build()
                                )
                                .withFilterPredicate(filterPredicate)
                                .buildBatchReadSession();
            }
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }

    }

    protected Predicate convertPredicate() throws UserException {
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
            com.aliyun.odps.table.optimizer.predicate.CompoundPredicate.Operator odpsOp = switch (
                    compoundPredicate.getOp()) {
                case AND -> com.aliyun.odps.table.optimizer.predicate.CompoundPredicate.Operator.AND;
                case OR -> com.aliyun.odps.table.optimizer.predicate.CompoundPredicate.Operator.OR;
                case NOT -> com.aliyun.odps.table.optimizer.predicate.CompoundPredicate.Operator.NOT;
            };
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

            List<Serializable> odpsValues = new ArrayList<>();



            String columnName = convertSlotRefToColumnName(expr.getChild(0));
            com.aliyun.odps.OdpsType odpsType  =  table.getColumnNameToOdpsColumn().get(columnName).getType();

            for (int i = 1; i < inPredicate.getChildren().size(); i++) {
                odpsValues.add(
                        new com.aliyun.odps.table.optimizer.predicate.Constant(
                        convertLiteralToOdpsValues(odpsType, expr.getChild(1))));
            }

            odpsPredicate = new com.aliyun.odps.table.optimizer.predicate.InPredicate(
                    odpsOp,
                    new com.aliyun.odps.table.optimizer.predicate.Attribute(columnName),
                    odpsValues
            );
        } else if (expr instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
            com.aliyun.odps.table.optimizer.predicate.BinaryPredicate.Operator odpsOp = switch (
                    binaryPredicate.getOp()) {
                case EQ -> com.aliyun.odps.table.optimizer.predicate.BinaryPredicate.Operator.EQUALS;
                case NE -> com.aliyun.odps.table.optimizer.predicate.BinaryPredicate.Operator.NOT_EQUALS;
                case GE -> com.aliyun.odps.table.optimizer.predicate.BinaryPredicate.Operator.GREATER_THAN;
                case LE -> com.aliyun.odps.table.optimizer.predicate.BinaryPredicate.Operator.LESS_THAN;
                case LT -> com.aliyun.odps.table.optimizer.predicate.BinaryPredicate.Operator.LESS_THAN_OR_EQUAL;
                case GT -> com.aliyun.odps.table.optimizer.predicate.BinaryPredicate.Operator.GREATER_THAN_OR_EQUAL;
                default -> null;
            };
            if (odpsOp != null) {


                String columnName = convertSlotRefToColumnName(expr.getChild(0));
                com.aliyun.odps.OdpsType odpsType  =  table.getColumnNameToOdpsColumn().get(columnName).getType();

                odpsPredicate =
                        new com.aliyun.odps.table.optimizer.predicate.BinaryPredicate(
                                odpsOp,
                                new com.aliyun.odps.table.optimizer.predicate.Attribute(columnName),
                                new com.aliyun.odps.table.optimizer.predicate.Constant(
                                        convertLiteralToOdpsValues(odpsType, expr.getChild(1)))
                        );
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
        if (!(expr instanceof SlotRef)) {
            throw new AnalysisException("Do not support convert ["
                    + expr.getExprName() + "] in convertSlotRefToAttribute.");
        }

        SlotRef slotRef = (SlotRef) expr;
        return slotRef.getColumnName();
    }

    private Object convertLiteralToOdpsValues(OdpsType odpsType, Expr expr) throws AnalysisException {
        if (!(expr instanceof LiteralExpr)) {
            throw new AnalysisException("Do not support convert ["
                    + expr.getExprName() + "] in convertSlotRefToAttribute.");
        }
        LiteralExpr literalExpr = (LiteralExpr) expr;

        Type literalExprType = literalExpr.getType().getResultType();
        switch (odpsType) {
            case BOOLEAN:
                if (!(literalExpr instanceof BoolLiteral)) {
                    break;
                }
                BoolLiteral boolLiteral = (BoolLiteral) literalExpr;
                return boolLiteral.getValue();
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT: {
                if (!literalExprType.isIntegerType()) {
                    break;
                }
                return  literalExpr.getRealValue();
            }
            case FLOAT:
            case DOUBLE: {
                if (!(literalExprType ==  Type.FLOAT || literalExprType == Type.DOUBLE)) {
                    break;
                }
                FloatLiteral floatLiteral = (FloatLiteral) literalExpr;
                return floatLiteral.getValue();
            }
            case STRING: {
                if (!(literalExprType ==  Type.STRING)) {
                    break;
                }
                StringLiteral stringLiteral = (StringLiteral) literalExpr;
                return stringLiteral.getValue();
            }
            case DECIMAL: {
                if (!(literalExprType.isWildcardDecimal())) {
                    break;
                }
                DecimalLiteral decimalLiteral = (DecimalLiteral) literalExpr;
                return decimalLiteral.getRealValue();
            }
            case CHAR: {
                if (!(literalExprType ==  Type.STRING)) {
                    break;
                }
                StringLiteral charLiteral = (StringLiteral) literalExpr;
                return new Char(charLiteral.getValue());
            }
            case VARCHAR: {
                if (!(literalExprType ==  Type.STRING)) {
                    break;
                }
                StringLiteral charLiteral = (StringLiteral) literalExpr;
                return new Varchar(charLiteral.getValue());
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

        try {
            InputSplitAssigner assigner = tableBatchReadSession.getInputSplitAssigner();

            if (splitStrategy.equals("byte_size"))  {
                for (com.aliyun.odps.table.read.split.InputSplit split : assigner.getAllSplits()) {

                    long modificationTime = table.getOdpsTable().getLastDataModifiedTime().getTime();
                    MaxComputeSplit maxComputeSplit =
                            new MaxComputeSplit(new LocationPath("/byte_size", Maps.newHashMap()),
                                    ((IndexedInputSplit) split).getSplitIndex(), -1,  -1,
                                    modificationTime, null,
                                    Collections.emptyList());


                    maxComputeSplit.scanSerialize = serialize(tableBatchReadSession);
                    maxComputeSplit.splitType = SplitType.BYTE_SIZE;
                    maxComputeSplit.sessionId = split.getSessionId();
                    result.add(maxComputeSplit);
                }
            } else if (splitStrategy.equals("row_offset")) {

                long totalRowCount =  assigner.getTotalRowCount();

                long recordsPerSplit = 4096;
                for (long offset = 0; offset < totalRowCount; offset += recordsPerSplit) {
                    recordsPerSplit = Math.min(recordsPerSplit, totalRowCount - offset);
                    com.aliyun.odps.table.read.split.InputSplit split =
                            assigner.getSplitByRowOffset(offset, recordsPerSplit);

                    long modificationTime = table.getOdpsTable().getLastDataModifiedTime().getTime();
                    MaxComputeSplit maxComputeSplit =
                            new MaxComputeSplit(new LocationPath("/row_offset", Maps.newHashMap()),
                            offset, recordsPerSplit, totalRowCount, modificationTime, null,
                            Collections.emptyList());

                    maxComputeSplit.scanSerialize = serialize(tableBatchReadSession);
                    maxComputeSplit.splitType = SplitType.ROW_OFFSET;
                    maxComputeSplit.sessionId = split.getSessionId();

                    result.add(maxComputeSplit);
                }
            } else {
                throw new RuntimeException("Unsupported split strategy: " + splitStrategy);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private static String serialize(Serializable object) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(object);
        byte[] serializedBytes = byteArrayOutputStream.toByteArray();
        return Base64.getEncoder().encodeToString(serializedBytes);
    }
}
