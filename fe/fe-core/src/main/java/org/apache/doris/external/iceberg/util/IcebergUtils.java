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

package org.apache.doris.external.iceberg.util;


import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Iceberg utils
 */
public class IcebergUtils {
    private static final Logger LOG = LogManager.getLogger(IcebergUtils.class);
    private static ThreadLocal<Integer> columnIdThreadLocal = new ThreadLocal<Integer>() {
        @Override
        public Integer initialValue() {
            return 0;
        }
    };

    /**
     * Create Iceberg schema from Doris ColumnDef.
     *
     * @param columnDefs columns for create iceberg table
     * @return Iceberg schema
     * @throws UserException if has aggregate type in create table statement
     */
    public static Schema createIcebergSchema(List<ColumnDef> columnDefs) throws UserException {
        columnIdThreadLocal.set(1);
        List<Types.NestedField> nestedFields = Lists.newArrayList();
        for (ColumnDef columnDef : columnDefs) {
            columnDef.analyze(false);
            if (columnDef.getAggregateType() != null) {
                throw new DdlException("Do not support aggregation column: " + columnDef.getName());
            }
            boolean isNullable = columnDef.isAllowNull();
            org.apache.iceberg.types.Type icebergType = convertDorisToIceberg(columnDef.getType());
            if (isNullable) {
                nestedFields.add(
                        Types.NestedField.optional(nextId(), columnDef.getName(), icebergType, columnDef.getComment()));
            } else {
                nestedFields.add(
                        Types.NestedField.required(nextId(), columnDef.getName(), icebergType, columnDef.getComment()));
            }
        }
        return new Schema(nestedFields);
    }

    public static List<Column> createSchemaFromIcebergSchema(Schema schema) throws DdlException {
        List<Column> columns = Lists.newArrayList();
        for (Types.NestedField nestedField : schema.columns()) {
            try {
                columns.add(nestedFieldToColumn(nestedField));
            } catch (UnsupportedOperationException e) {
                if (Config.iceberg_table_creation_strict_mode) {
                    throw e;
                }
                LOG.warn("Unsupported data type in Doris, ignore column[{}], with error: {}",
                        nestedField.name(), e.getMessage());
                continue;
            }
        }
        return columns;
    }

    public static Column nestedFieldToColumn(Types.NestedField field) {
        Type type = convertIcebergToDoris(field.type());
        return new Column(field.name(), type, true, null, field.isOptional(), null, field.doc());
    }

    /**
     * get iceberg table schema id to name mapping
     *
     * @param schema iceberg table schema
     * @return id to name mapping
     */
    public static Map<Integer, String> getIdToName(Schema schema) {
        Map<Integer, String> idToName = new HashMap<>();
        for (Types.NestedField nestedField : schema.columns()) {
            idToName.put(nestedField.fieldId(), nestedField.name());
        }
        return idToName;
    }

    public static List<String> getIdentityPartitionField(PartitionSpec spec) {
        return PartitionSpecVisitor.visit(spec,
                new PartitionSpecVisitor<String>() {
                    @Override
                    public String identity(String sourceName, int sourceId) {
                        return sourceName;
                    }

                    @Override
                    public String bucket(String sourceName, int sourceId, int numBuckets) {
                        return null;
                    }

                    @Override
                    public String truncate(String sourceName, int sourceId, int width) {
                        return null;
                    }

                    @Override
                    public String year(String sourceName, int sourceId) {
                        return null;
                    }

                    @Override
                    public String month(String sourceName, int sourceId) {
                        return null;
                    }

                    @Override
                    public String day(String sourceName, int sourceId) {
                        return null;
                    }

                    @Override
                    public String hour(String sourceName, int sourceId) {
                        return null;
                    }

                    @Override
                    public String alwaysNull(int fieldId, String sourceName, int sourceId) {
                        return null;
                    }

                    @Override
                    public String unknown(int fieldId, String sourceName, int sourceId, String transform) {
                        return null;
                    }
                }
        ).stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    /**
     * Convert a {@link org.apache.iceberg.types.Type} to a {@link Type doris type}.
     *
     * @param type a iceberg Type
     * @return the equivalent doris type
     * @throws IllegalArgumentException if the type cannot be converted to doris
     */
    public static Type convertIcebergToDoris(org.apache.iceberg.types.Type type) {
        return TypeUtil.visit(type, new TypeToDorisType());
    }

    /**
     * Convert a doris {@link Type struct} to a {@link org.apache.iceberg.types.Type} with new field ids.
     * <p>
     * This conversion assigns fresh ids.
     * <p>
     * Some data types are represented as the same doris type. These are converted to a default type.
     *
     * @param type a doris Type
     * @return the equivalent Type
     * @throws IllegalArgumentException if the type cannot be converted
     */
    public static org.apache.iceberg.types.Type convertDorisToIceberg(Type type) {
        return DorisTypeVisitor.visit(type, new DorisTypeToType());
    }

    public static Expression convertToIcebergExpr(Expr expr) {
        if (expr == null) {
            return null;
        }

        // BoolLiteral
        if (expr instanceof BoolLiteral) {
            BoolLiteral boolLiteral = (BoolLiteral) expr;
            boolean value = boolLiteral.getValue();
            if (value) {
                return Expressions.alwaysTrue();
            } else {
                return Expressions.alwaysFalse();
            }
        }

        // CompoundPredicate
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
            switch (compoundPredicate.getOp()) {
                case AND: {
                    Expression left = convertToIcebergExpr(compoundPredicate.getChild(0));
                    Expression right = convertToIcebergExpr(compoundPredicate.getChild(1));
                    if (left != null && right != null) {
                        return Expressions.and(left, right);
                    }
                    return null;
                }
                case OR: {
                    Expression left = convertToIcebergExpr(compoundPredicate.getChild(0));
                    Expression right = convertToIcebergExpr(compoundPredicate.getChild(1));
                    if (left != null && right != null) {
                        return Expressions.or(left, right);
                    }
                    return null;
                }
                case NOT: {
                    Expression child = convertToIcebergExpr(compoundPredicate.getChild(0));
                    if (child != null) {
                        return Expressions.not(child);
                    }
                    return null;
                }
                default:
                    return null;
            }
        }

        TExprOpcode opCode = expr.getOpcode();
        switch (opCode) {
            case EQ:
            case NE:
            case GE:
            case GT:
            case LE:
            case LT:
            case EQ_FOR_NULL:
                BinaryPredicate eq = (BinaryPredicate) expr;
                SlotRef slotRef = convertDorisExprToSlotRef(eq.getChild(0));
                LiteralExpr literalExpr = null;
                if (slotRef == null && eq.getChild(0).isLiteral()) {
                    literalExpr = (LiteralExpr) eq.getChild(0);
                    slotRef = convertDorisExprToSlotRef(eq.getChild(1));
                } else if (eq.getChild(1).isLiteral()) {
                    literalExpr = (LiteralExpr) eq.getChild(1);
                }
                if (slotRef == null || literalExpr == null) {
                    return null;
                }
                String colName = slotRef.getColumnName();
                Object value = extractDorisLiteral(literalExpr);
                if (value == null) {
                    if (opCode == TExprOpcode.EQ_FOR_NULL && literalExpr instanceof NullLiteral) {
                        return Expressions.isNull(colName);
                    } else {
                        return null;
                    }
                }
                switch (opCode) {
                    case EQ:
                    case EQ_FOR_NULL:
                        return Expressions.equal(colName, value);
                    case NE:
                        return Expressions.not(Expressions.equal(colName, value));
                    case GE:
                        return Expressions.greaterThanOrEqual(colName, value);
                    case GT:
                        return Expressions.greaterThan(colName, value);
                    case LE:
                        return Expressions.lessThanOrEqual(colName, value);
                    case LT:
                        return Expressions.lessThan(colName, value);
                    default:
                        return null;
                }
            default:
                return null;
        }
    }

    private static Object extractDorisLiteral(Expr expr) {
        if (!expr.isLiteral()) {
            return null;
        }
        if (expr instanceof BoolLiteral) {
            BoolLiteral boolLiteral = (BoolLiteral) expr;
            return boolLiteral.getValue();
        } else if (expr instanceof DateLiteral) {
            DateLiteral dateLiteral = (DateLiteral) expr;
            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
            StringBuilder sb = new StringBuilder();
            sb.append(dateLiteral.getYear())
                    .append(dateLiteral.getMonth())
                    .append(dateLiteral.getDay())
                    .append(dateLiteral.getHour())
                    .append(dateLiteral.getMinute())
                    .append(dateLiteral.getSecond());
            Date date;
            try {
                date = formatter.parse(sb.toString());
            } catch (ParseException e) {
                return null;
            }
            return date.getTime();
        } else if (expr instanceof DecimalLiteral) {
            DecimalLiteral decimalLiteral = (DecimalLiteral) expr;
            return decimalLiteral.getValue();
        } else if (expr instanceof FloatLiteral) {
            FloatLiteral floatLiteral = (FloatLiteral) expr;
            return floatLiteral.getValue();
        } else if (expr instanceof IntLiteral) {
            IntLiteral intLiteral = (IntLiteral) expr;
            return intLiteral.getValue();
        } else if (expr instanceof StringLiteral) {
            StringLiteral stringLiteral = (StringLiteral) expr;
            return stringLiteral.getStringValue();
        }
        return null;
    }

    private static SlotRef convertDorisExprToSlotRef(Expr expr) {
        SlotRef slotRef = null;
        if (expr instanceof SlotRef) {
            slotRef = (SlotRef) expr;
        } else if (expr instanceof CastExpr) {
            if (expr.getChild(0) instanceof SlotRef) {
                slotRef = (SlotRef) expr.getChild(0);
            }
        }
        return slotRef;
    }

    private static int findWidth(IntLiteral literal) {
        Preconditions.checkArgument(literal.getValue() > 0 && literal.getValue() < Integer.MAX_VALUE,
                "Unsupported width " + literal.getValue());
        return (int) literal.getValue();
    }

    public static int nextId() {
        int nextId = columnIdThreadLocal.get();
        columnIdThreadLocal.set(nextId + 1);
        return nextId;
    }

    public static Set<String> getAllDataFilesPath(org.apache.iceberg.Table table, TableOperations ops) {
        org.apache.iceberg.Table dataFilesTable = MetadataTableUtils.createMetadataTableInstance(
                ops, table.name(), table.name(), MetadataTableType.ALL_DATA_FILES);

        Set<String> dataFilesPath = Sets.newHashSet();
        TableScan tableScan = dataFilesTable.newScan();
        List<CombinedScanTask> tasks = Lists.newArrayList(tableScan.planTasks());
        tasks.forEach(task ->
                task.files().forEach(fileScanTask -> {
                    Lists.newArrayList(fileScanTask.asDataTask().rows())
                            .forEach(row -> dataFilesPath.add(row.get(1, String.class)));
                })
        );

        return dataFilesPath;
    }

    public static PartitionSpec buildPartitionSpec(Schema schema, List<String> partitionNames) {
        if (partitionNames == null || partitionNames.isEmpty()) {
            return null;
        }
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        for (String partitionName : partitionNames) {
            builder.identity(partitionName);
        }
        return builder.build();
    }

}
