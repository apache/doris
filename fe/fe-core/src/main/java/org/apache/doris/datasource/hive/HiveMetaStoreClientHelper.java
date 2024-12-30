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

package org.apache.doris.datasource.hive;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CastExpr;
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
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Helper class for HiveMetaStoreClient
 */
public class HiveMetaStoreClientHelper {
    private static final Logger LOG = LogManager.getLogger(HiveMetaStoreClientHelper.class);

    public static final String COMMENT = "comment";

    private static final Pattern digitPattern = Pattern.compile("(\\d+)");

    public enum HiveFileFormat {
        TEXT_FILE(0, "text"),
        PARQUET(1, "parquet"),
        ORC(2, "orc");

        private int index;
        private String desc;

        HiveFileFormat(int index, String desc) {
            this.index = index;
            this.desc = desc;
        }

        public int getIndex() {
            return index;
        }

        public String getDesc() {
            return desc;
        }

        /**
         * convert Hive table inputFormat to file format
         * @param input inputFormat of Hive file
         * @return
         * @throws DdlException
         */
        public static String getFormat(String input) throws DdlException {
            String formatDesc = "";
            for (HiveFileFormat format : HiveFileFormat.values()) {
                String lowerCaseInput = input.toLowerCase();
                if (lowerCaseInput.contains(format.getDesc())) {
                    formatDesc = format.getDesc();
                    break;
                }
            }
            if (Strings.isNullOrEmpty(formatDesc)) {
                LOG.warn("Not supported Hive file format [{}].", input);
                throw new DdlException("Not supported Hive file format " + input);
            }
            return formatDesc;
        }
    }

    /**
     * Convert Doris expr to Hive expr, only for partition column
     * @param tblName
     * @return
     * @throws DdlException
     * @throws SemanticException
     */
    public static ExprNodeGenericFuncDesc convertToHivePartitionExpr(List<Expr> conjuncts,
            List<String> partitionKeys, String tblName) throws DdlException {
        List<ExprNodeDesc> hivePredicates = new ArrayList<>();

        for (Expr conjunct : conjuncts) {
            ExprNodeGenericFuncDesc hiveExpr = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                    conjunct, partitionKeys, tblName).getFuncDesc();
            if (hiveExpr != null) {
                hivePredicates.add(hiveExpr);
            }
        }
        int count = hivePredicates.size();
        // combine all predicate by `and`
        // compoundExprs must have at least 2 predicates
        if (count >= 2) {
            return HiveMetaStoreClientHelper.getCompoundExpr(hivePredicates, "and");
        } else if (count == 1) {
            // only one predicate
            return (ExprNodeGenericFuncDesc) hivePredicates.get(0);
        } else {
            return genAlwaysTrueExpr(tblName);
        }
    }

    private static ExprNodeGenericFuncDesc genAlwaysTrueExpr(String tblName) throws DdlException {
        // have no predicate, make a dummy predicate "1=1" to get all partitions
        HiveMetaStoreClientHelper.ExprBuilder exprBuilder =
                new HiveMetaStoreClientHelper.ExprBuilder(tblName);
        return exprBuilder.val(TypeInfoFactory.intTypeInfo, 1)
                .val(TypeInfoFactory.intTypeInfo, 1)
                .pred("=", 2).build();
    }

    private static class ExprNodeGenericFuncDescContext {
        private static final ExprNodeGenericFuncDescContext BAD_CONTEXT = new ExprNodeGenericFuncDescContext();

        private ExprNodeGenericFuncDesc funcDesc = null;
        private boolean eligible = false;

        public ExprNodeGenericFuncDescContext(ExprNodeGenericFuncDesc funcDesc) {
            this.funcDesc = funcDesc;
            this.eligible = true;
        }

        private ExprNodeGenericFuncDescContext() {
        }

        /**
         * Check eligible before use the expr in CompoundPredicate for `and` and `or` .
         */
        public boolean isEligible() {
            return eligible;
        }

        public ExprNodeGenericFuncDesc getFuncDesc() {
            return funcDesc;
        }
    }

    private static ExprNodeGenericFuncDescContext convertToHivePartitionExpr(Expr dorisExpr,
            List<String> partitionKeys, String tblName) throws DdlException {
        if (dorisExpr == null) {
            return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
        }

        if (dorisExpr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) dorisExpr;
            ExprNodeGenericFuncDescContext left = convertToHivePartitionExpr(
                    compoundPredicate.getChild(0), partitionKeys, tblName);
            ExprNodeGenericFuncDescContext right = convertToHivePartitionExpr(
                    compoundPredicate.getChild(1), partitionKeys, tblName);

            switch (compoundPredicate.getOp()) {
                case AND: {
                    if (left.isEligible() && right.isEligible()) {
                        List<ExprNodeDesc> andArgs = new ArrayList<>();
                        andArgs.add(left.getFuncDesc());
                        andArgs.add(right.getFuncDesc());
                        return new ExprNodeGenericFuncDescContext(getCompoundExpr(andArgs, "and"));
                    } else if (left.isEligible()) {
                        return left;
                    } else if (right.isEligible()) {
                        return right;
                    } else {
                        return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
                    }
                }
                case OR: {
                    if (left.isEligible() && right.isEligible()) {
                        List<ExprNodeDesc> andArgs = new ArrayList<>();
                        andArgs.add(left.getFuncDesc());
                        andArgs.add(right.getFuncDesc());
                        return new ExprNodeGenericFuncDescContext(getCompoundExpr(andArgs, "or"));
                    } else {
                        // If it is not a partition key, this is an always true expr.
                        // Or if is a partition key and also is a not supportedOp, this is an always true expr.
                        return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
                    }
                }
                default:
                    // TODO: support NOT predicate for CompoundPredicate
                    return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
            }
        }
        return binaryExprDesc(dorisExpr, partitionKeys, tblName);
    }

    private static ExprNodeGenericFuncDescContext binaryExprDesc(Expr dorisExpr,
            List<String> partitionKeys, String tblName) throws DdlException {
        TExprOpcode opcode = dorisExpr.getOpcode();
        switch (opcode) {
            case EQ:
            case NE:
            case GE:
            case GT:
            case LE:
            case LT:
            case EQ_FOR_NULL:
                BinaryPredicate eq = (BinaryPredicate) dorisExpr;
                // Make sure the col slot is always first
                SlotRef slotRef = convertDorisExprToSlotRef(eq.getChild(0));
                LiteralExpr literalExpr = convertDorisExprToLiteralExpr(eq.getChild(1));
                if (slotRef == null || literalExpr == null) {
                    return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
                }
                String colName = slotRef.getColumnName();
                // check whether colName is partition column or not
                if (!partitionKeys.contains(colName)) {
                    return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
                }
                PrimitiveType dorisPrimitiveType = slotRef.getType().getPrimitiveType();
                PrimitiveTypeInfo hivePrimitiveType = convertToHiveColType(dorisPrimitiveType);
                Object value = extractDorisLiteral(literalExpr);
                if (value == null) {
                    if (opcode == TExprOpcode.EQ_FOR_NULL && literalExpr instanceof NullLiteral) {
                        return genExprDesc(tblName, hivePrimitiveType, colName, "NULL", "=");
                    } else {
                        return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
                    }
                }
                switch (opcode) {
                    case EQ:
                    case EQ_FOR_NULL:
                        return genExprDesc(tblName, hivePrimitiveType, colName, value, "=");
                    case NE:
                        return genExprDesc(tblName, hivePrimitiveType, colName, value, "!=");
                    case GE:
                        return genExprDesc(tblName, hivePrimitiveType, colName, value, ">=");
                    case GT:
                        return genExprDesc(tblName, hivePrimitiveType, colName, value, ">");
                    case LE:
                        return genExprDesc(tblName, hivePrimitiveType, colName, value, "<=");
                    case LT:
                        return genExprDesc(tblName, hivePrimitiveType, colName, value, "<");
                    default:
                        return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
                }
            default:
                // TODO: support in predicate
                return ExprNodeGenericFuncDescContext.BAD_CONTEXT;
        }
    }

    private static ExprNodeGenericFuncDescContext genExprDesc(
            String tblName,
            PrimitiveTypeInfo hivePrimitiveType,
            String colName,
            Object value,
            String op) throws DdlException {
        ExprBuilder exprBuilder = new ExprBuilder(tblName);
        exprBuilder.col(hivePrimitiveType, colName).val(hivePrimitiveType, value);
        return new ExprNodeGenericFuncDescContext(exprBuilder.pred(op, 2).build());
    }

    public static ExprNodeGenericFuncDesc getCompoundExpr(List<ExprNodeDesc> args, String op) throws DdlException {
        ExprNodeGenericFuncDesc compoundExpr;
        try {
            compoundExpr = ExprNodeGenericFuncDesc.newInstance(
                    FunctionRegistry.getFunctionInfo(op).getGenericUDF(), args);
        } catch (SemanticException e) {
            LOG.warn("Convert to Hive expr failed: {}", e.getMessage());
            throw new DdlException("Convert to Hive expr failed. Error: " + e.getMessage());
        }
        return compoundExpr;
    }

    public static SlotRef convertDorisExprToSlotRef(Expr expr) {
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

    public static LiteralExpr convertDorisExprToLiteralExpr(Expr expr) {
        LiteralExpr literalExpr = null;
        if (expr instanceof LiteralExpr) {
            literalExpr = (LiteralExpr) expr;
        } else if (expr instanceof CastExpr) {
            if (expr.getChild(0) instanceof LiteralExpr) {
                literalExpr = (LiteralExpr) expr.getChild(0);
            }
        }
        return literalExpr;
    }

    public static Object extractDorisLiteral(Expr expr) {
        if (!expr.isLiteral()) {
            return null;
        }
        if (expr instanceof BoolLiteral) {
            BoolLiteral boolLiteral = (BoolLiteral) expr;
            return boolLiteral.getValue();
        } else if (expr instanceof DateLiteral) {
            DateLiteral dateLiteral = (DateLiteral) expr;
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
                    .withZone(ZoneId.systemDefault());
            StringBuilder sb = new StringBuilder();
            sb.append(dateLiteral.getYear())
                    .append(dateLiteral.getMonth())
                    .append(dateLiteral.getDay())
                    .append(dateLiteral.getHour())
                    .append(dateLiteral.getMinute())
                    .append(dateLiteral.getSecond());
            Date date;
            try {
                date = Date.from(
                        LocalDateTime.parse(sb.toString(), formatter).atZone(ZoneId.systemDefault()).toInstant());
            } catch (DateTimeParseException e) {
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

    /**
     * Convert from Doris column type to Hive column type
     * @param dorisType
     * @return hive primitive type info
     * @throws DdlException
     */
    private static PrimitiveTypeInfo convertToHiveColType(PrimitiveType dorisType) throws DdlException {
        switch (dorisType) {
            case BOOLEAN:
                return TypeInfoFactory.booleanTypeInfo;
            case TINYINT:
                return TypeInfoFactory.byteTypeInfo;
            case SMALLINT:
                return TypeInfoFactory.shortTypeInfo;
            case INT:
                return TypeInfoFactory.intTypeInfo;
            case BIGINT:
                return TypeInfoFactory.longTypeInfo;
            case FLOAT:
                return TypeInfoFactory.floatTypeInfo;
            case DOUBLE:
                return TypeInfoFactory.doubleTypeInfo;
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMALV2:
                return TypeInfoFactory.decimalTypeInfo;
            case DATE:
            case DATEV2:
                return TypeInfoFactory.dateTypeInfo;
            case DATETIME:
            case DATETIMEV2:
                return TypeInfoFactory.timestampTypeInfo;
            case CHAR:
                return TypeInfoFactory.charTypeInfo;
            case VARCHAR:
            case STRING:
                return TypeInfoFactory.varcharTypeInfo;
            default:
                throw new DdlException("Unsupported column type: " + dorisType);
        }
    }

    /**
     * Helper class for building a Hive expression.
     */
    public static class ExprBuilder {
        private final String tblName;
        private final Deque<ExprNodeDesc> queue = new LinkedList<>();

        public ExprBuilder(String tblName) {
            this.tblName = tblName;
        }

        public ExprNodeGenericFuncDesc build() throws DdlException {
            if (queue.size() != 1) {
                throw new DdlException("Build Hive expression Failed: " + queue.size());
            }
            return (ExprNodeGenericFuncDesc) queue.pollFirst();
        }

        public ExprBuilder pred(String name, int args) throws DdlException {
            return fn(name, TypeInfoFactory.booleanTypeInfo, args);
        }

        private ExprBuilder fn(String name, TypeInfo ti, int args) throws DdlException {
            List<ExprNodeDesc> children = new ArrayList<>();
            for (int i = 0; i < args; ++i) {
                children.add(queue.pollFirst());
            }
            try {
                queue.offerLast(new ExprNodeGenericFuncDesc(ti,
                        FunctionRegistry.getFunctionInfo(name).getGenericUDF(), children));
            } catch (SemanticException e) {
                LOG.warn("Build Hive expression failed: semantic analyze exception: {}", e.getMessage());
                throw new DdlException("Build Hive expression Failed. Error: " + e.getMessage());
            }
            return this;
        }

        public ExprBuilder col(TypeInfo ti, String col) {
            queue.offerLast(new ExprNodeColumnDesc(ti, col, tblName, true));
            return this;
        }

        public ExprBuilder val(TypeInfo ti, Object val) {
            queue.offerLast(new ExprNodeConstantDesc(ti, val));
            return this;
        }
    }

    /**
     * The nested column has inner columns, and each column is separated a comma. The inner column maybe a nested
     * column too, so we cannot simply split by the comma. We need to match the angle brackets，
     * and deal with the inner column recursively.
     */
    private static int findNextNestedField(String commaSplitFields) {
        int numLess = 0;
        int numBracket = 0;
        for (int i = 0; i < commaSplitFields.length(); i++) {
            char c = commaSplitFields.charAt(i);
            if (c == '<') {
                numLess++;
            } else if (c == '>') {
                numLess--;
            } else if (c == '(') {
                numBracket++;
            } else if (c == ')') {
                numBracket--;
            } else if (c == ',' && numLess == 0 && numBracket == 0) {
                return i;
            }
        }
        return commaSplitFields.length();
    }

    /**
     * Convert doris type to hive type.
     */
    public static String dorisTypeToHiveType(Type dorisType) {
        if (dorisType.isScalarType()) {
            PrimitiveType primitiveType = dorisType.getPrimitiveType();
            switch (primitiveType) {
                case BOOLEAN:
                    return "boolean";
                case TINYINT:
                    return "tinyint";
                case SMALLINT:
                    return "smallint";
                case INT:
                    return "int";
                case BIGINT:
                    return "bigint";
                case DATEV2:
                case DATE:
                    return "date";
                case DATETIMEV2:
                case DATETIME:
                    return "timestamp";
                case FLOAT:
                    return "float";
                case DOUBLE:
                    return "double";
                case CHAR: {
                    ScalarType scalarType = (ScalarType) dorisType;
                    return "char(" + scalarType.getLength() + ")";
                }
                case VARCHAR:
                case STRING:
                    return "string";
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMAL256:
                case DECIMALV2: {
                    StringBuilder decimalType = new StringBuilder();
                    decimalType.append("decimal");
                    ScalarType scalarType = (ScalarType) dorisType;
                    int precision = scalarType.getScalarPrecision();
                    if (precision == 0) {
                        precision = ScalarType.DEFAULT_PRECISION;
                    }
                    // decimal(precision, scale)
                    int scale = scalarType.getScalarScale();
                    decimalType.append("(");
                    decimalType.append(precision);
                    decimalType.append(",");
                    decimalType.append(scale);
                    decimalType.append(")");
                    return decimalType.toString();
                }
                default:
                    throw new HMSClientException("Unsupported primitive type conversion of " + dorisType.toSql());
            }
        } else if (dorisType.isArrayType()) {
            ArrayType dorisArray = (ArrayType) dorisType;
            Type itemType = dorisArray.getItemType();
            return "array<" + dorisTypeToHiveType(itemType) + ">";
        } else if (dorisType.isMapType()) {
            MapType dorisMap = (MapType) dorisType;
            Type keyType = dorisMap.getKeyType();
            Type valueType = dorisMap.getValueType();
            return "map<"
                    + dorisTypeToHiveType(keyType)
                    + ","
                    + dorisTypeToHiveType(valueType)
                    + ">";
        } else if (dorisType.isStructType()) {
            StructType dorisStruct = (StructType) dorisType;
            StringBuilder structType = new StringBuilder();
            structType.append("struct<");
            ArrayList<StructField> fields = dorisStruct.getFields();
            for (int i = 0; i < fields.size(); i++) {
                StructField field = fields.get(i);
                structType.append(field.getName());
                structType.append(":");
                structType.append(dorisTypeToHiveType(field.getType()));
                if (i != fields.size() - 1) {
                    structType.append(",");
                }
            }
            structType.append(">");
            return structType.toString();
        }
        throw new HMSClientException("Unsupported type conversion of " + dorisType.toSql());
    }

    /**
     * Convert hive type to doris type.
     */
    public static Type hiveTypeToDorisType(String hiveType) {
        // use the largest scale as default time scale.
        return hiveTypeToDorisType(hiveType, 6);
    }

    /**
     * Convert hive type to doris type with timescale.
     */
    public static Type hiveTypeToDorisType(String hiveType, int timeScale) {
        String lowerCaseType = hiveType.toLowerCase();
        switch (lowerCaseType) {
            case "boolean":
                return Type.BOOLEAN;
            case "tinyint":
                return Type.TINYINT;
            case "smallint":
                return Type.SMALLINT;
            case "int":
                return Type.INT;
            case "bigint":
                return Type.BIGINT;
            case "date":
                return ScalarType.createDateV2Type();
            case "timestamp":
                return ScalarType.createDatetimeV2Type(timeScale);
            case "float":
                return Type.FLOAT;
            case "double":
                return Type.DOUBLE;
            case "string":
            case "binary":
                return ScalarType.createStringType();
            default:
                break;
        }
        // resolve schema like array<int>
        if (lowerCaseType.startsWith("array")) {
            if (lowerCaseType.indexOf("<") == 5 && lowerCaseType.lastIndexOf(">") == lowerCaseType.length() - 1) {
                Type innerType = hiveTypeToDorisType(lowerCaseType.substring(6, lowerCaseType.length() - 1));
                return ArrayType.create(innerType, true);
            }
        }
        // resolve schema like map<text, int>
        if (lowerCaseType.startsWith("map")) {
            if (lowerCaseType.indexOf("<") == 3 && lowerCaseType.lastIndexOf(">") == lowerCaseType.length() - 1) {
                String keyValue = lowerCaseType.substring(4, lowerCaseType.length() - 1);
                int index = findNextNestedField(keyValue);
                if (index != keyValue.length() && index != 0) {
                    return new MapType(hiveTypeToDorisType(keyValue.substring(0, index)),
                            hiveTypeToDorisType(keyValue.substring(index + 1)));
                }
            }
        }
        // resolve schema like struct<col1: text, col2: int>
        if (lowerCaseType.startsWith("struct")) {
            if (lowerCaseType.indexOf("<") == 6 && lowerCaseType.lastIndexOf(">") == lowerCaseType.length() - 1) {
                String listFields = lowerCaseType.substring(7, lowerCaseType.length() - 1);
                ArrayList<StructField> fields = new ArrayList<>();
                while (listFields.length() > 0) {
                    int index = findNextNestedField(listFields);
                    int pivot = listFields.indexOf(':');
                    if (pivot > 0 && pivot < listFields.length() - 1) {
                        fields.add(new StructField(listFields.substring(0, pivot),
                                hiveTypeToDorisType(listFields.substring(pivot + 1, index))));
                        listFields = listFields.substring(Math.min(index + 1, listFields.length()));
                    } else {
                        break;
                    }
                }
                if (listFields.isEmpty()) {
                    return new StructType(fields);
                }
            }
        }
        if (lowerCaseType.startsWith("char")) {
            Matcher match = digitPattern.matcher(lowerCaseType);
            if (match.find()) {
                return ScalarType.createType(PrimitiveType.CHAR, Integer.parseInt(match.group(1)), 0, 0);
            }
            return ScalarType.createType(PrimitiveType.CHAR);
        }
        if (lowerCaseType.startsWith("varchar")) {
            Matcher match = digitPattern.matcher(lowerCaseType);
            if (match.find()) {
                return ScalarType.createType(PrimitiveType.VARCHAR, Integer.parseInt(match.group(1)), 0, 0);
            }
            return ScalarType.createType(PrimitiveType.VARCHAR);
        }
        if (lowerCaseType.startsWith("decimal")) {
            Matcher match = digitPattern.matcher(lowerCaseType);
            int precision = ScalarType.DEFAULT_PRECISION;
            int scale = ScalarType.DEFAULT_SCALE;
            if (match.find()) {
                precision = Integer.parseInt(match.group(1));
            }
            if (match.find()) {
                scale = Integer.parseInt(match.group(1));
            }
            return ScalarType.createDecimalV3Type(precision, scale);
        }
        return Type.UNSUPPORTED;
    }

    public static String showCreateTable(org.apache.hadoop.hive.metastore.api.Table remoteTable) {
        StringBuilder output = new StringBuilder();
        if (remoteTable.isSetViewOriginalText() || remoteTable.isSetViewExpandedText()) {
            output.append(String.format("CREATE VIEW `%s` AS ", remoteTable.getTableName()));
            if (remoteTable.getViewExpandedText() != null) {
                output.append(remoteTable.getViewExpandedText());
            } else {
                output.append(remoteTable.getViewOriginalText());
            }
        } else {
            output.append(String.format("CREATE TABLE `%s`(\n", remoteTable.getTableName()));
            Iterator<FieldSchema> fields = remoteTable.getSd().getCols().iterator();
            while (fields.hasNext()) {
                FieldSchema field = fields.next();
                output.append(String.format("  `%s` %s", field.getName(), field.getType()));
                if (field.getComment() != null) {
                    output.append(String.format(" COMMENT '%s'", field.getComment()));
                }
                if (fields.hasNext()) {
                    output.append(",\n");
                }
            }
            output.append(")\n");
            if (remoteTable.getParameters().containsKey(COMMENT)) {
                output.append(String.format("COMMENT '%s'", remoteTable.getParameters().get(COMMENT))).append("\n");
            }
            if (remoteTable.getPartitionKeys().size() > 0) {
                output.append("PARTITIONED BY (\n")
                        .append(remoteTable.getPartitionKeys().stream().map(
                                        partition ->
                                                String.format(" `%s` %s", partition.getName(), partition.getType()))
                                .collect(Collectors.joining(",\n")))
                        .append(")\n");
            }
            StorageDescriptor descriptor = remoteTable.getSd();
            List<String> bucketCols = descriptor.getBucketCols();
            if (bucketCols != null && bucketCols.size() > 0) {
                output.append("CLUSTERED BY (\n")
                        .append(bucketCols.stream().map(
                                bucketCol -> "  " + bucketCol).collect(Collectors.joining(",\n")))
                        .append(")\n")
                        .append(String.format("INTO %d BUCKETS\n", descriptor.getNumBuckets()));
            }
            if (descriptor.getSerdeInfo().isSetSerializationLib()) {
                output.append("ROW FORMAT SERDE\n")
                        .append(String.format("  '%s'\n", descriptor.getSerdeInfo().getSerializationLib()));
            }
            if (descriptor.getSerdeInfo().isSetParameters()) {
                output.append("WITH SERDEPROPERTIES (\n")
                        .append(descriptor.getSerdeInfo().getParameters().entrySet().stream()
                        .map(entry -> String.format("  '%s' = '%s'", entry.getKey(), entry.getValue()))
                        .collect(Collectors.joining(",\n")))
                        .append(")\n");
            }
            if (descriptor.isSetInputFormat()) {
                output.append("STORED AS INPUTFORMAT\n")
                        .append(String.format("  '%s'\n", descriptor.getInputFormat()));
            }
            if (descriptor.isSetOutputFormat()) {
                output.append("OUTPUTFORMAT\n")
                        .append(String.format("  '%s'\n", descriptor.getOutputFormat()));
            }
            if (descriptor.isSetLocation()) {
                output.append("LOCATION\n")
                        .append(String.format("  '%s'\n", descriptor.getLocation()));
            }
            if (remoteTable.isSetParameters()) {
                output.append("TBLPROPERTIES (\n");
                Map<String, String> parameters = Maps.newHashMap();
                // Copy the parameters to a new Map to keep them unchanged.
                parameters.putAll(remoteTable.getParameters());
                if (parameters.containsKey(COMMENT)) {
                    // Comment is always added to the end of remote table parameters.
                    // It has already showed above in COMMENT section, so remove it here.
                    parameters.remove(COMMENT);
                }
                Iterator<Map.Entry<String, String>> params = parameters.entrySet().iterator();
                while (params.hasNext()) {
                    Map.Entry<String, String> param = params.next();
                    output.append(String.format("  '%s'='%s'", param.getKey(), param.getValue()));
                    if (params.hasNext()) {
                        output.append(",\n");
                    }
                }
                output.append(")");
            }
        }
        return output.toString();
    }

    public static Schema getHudiTableSchema(HMSExternalTable table) {
        HoodieTableMetaClient metaClient = table.getHudiClient();
        TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
        Schema hudiSchema;

        // Here, the timestamp should be reloaded again.
        // Because when hudi obtains the schema in `getTableAvroSchema`, it needs to read the specified commit file,
        // which is saved in the `metaClient`.
        // But the `metaClient` is obtained from cache, so the file obtained may be an old file.
        // This file may be deleted by hudi clean task, and an error will be reported.
        // So, we should reload timeline so that we can read the latest commit files.
        metaClient.reloadActiveTimeline();

        try {
            hudiSchema = HoodieAvroUtils.createHoodieWriteSchema(schemaUtil.getTableAvroSchema());
        } catch (Exception e) {
            throw new RuntimeException("Cannot get hudi table schema.", e);
        }
        return hudiSchema;
    }


    public static <T> T ugiDoAs(Configuration conf, PrivilegedExceptionAction<T> action) {
        // if hive config is not ready, then use hadoop kerberos to login
        AuthenticationConfig authenticationConfig = AuthenticationConfig.getKerberosConfig(conf);
        HadoopAuthenticator hadoopAuthenticator = HadoopAuthenticator.getHadoopAuthenticator(authenticationConfig);
        try {
            return hadoopAuthenticator.doAs(action);
        } catch (IOException e) {
            LOG.warn("HiveMetaStoreClientHelper ugiDoAs failed.", e);
            throw new RuntimeException(e);
        }
    }

    public static Configuration getConfiguration(HMSExternalTable table) {
        return table.getCatalog().getConfiguration();
    }

    public static Optional<String> getSerdeProperty(Table table, String key) {
        String valueFromSd = table.getSd().getSerdeInfo().getParameters().get(key);
        String valueFromTbl = table.getParameters().get(key);
        return firstNonNullable(valueFromTbl, valueFromSd);
    }

    private static Optional<String> firstNonNullable(String... values) {
        for (String value : values) {
            if (!Strings.isNullOrEmpty(value)) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }

    public static String firstPresentOrDefault(String defaultValue, Optional<String>... values) {
        for (Optional<String> value : values) {
            if (value.isPresent()) {
                return value.get();
            }
        }
        return defaultValue;
    }

    /**
     * Return the byte value of the number string.
     *
     * @param altValue
     *                 The string containing a number.
     */
    public static String getByte(String altValue) {
        if (altValue != null && altValue.length() > 0) {
            try {
                return String.valueOf((char) ((Byte.parseByte(altValue) + 256) % 256));
            } catch (NumberFormatException e) {
                return altValue.substring(0, 1);
            }
        }
        return null;
    }
}
