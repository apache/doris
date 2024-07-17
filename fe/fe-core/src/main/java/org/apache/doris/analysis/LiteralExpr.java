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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/LiteralExpr.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.MysqlColType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public abstract class LiteralExpr extends Expr implements Comparable<LiteralExpr> {
    private static final Logger LOG = LogManager.getLogger(LiteralExpr.class);

    public LiteralExpr() {
        numDistinctValues = 1;
    }

    protected LiteralExpr(LiteralExpr other) {
        super(other);
    }

    public static LiteralExpr create(String value, Type type) throws AnalysisException {
        Preconditions.checkArgument(!type.equals(Type.INVALID));
        LiteralExpr literalExpr = null;
        switch (type.getPrimitiveType()) {
            case NULL_TYPE:
                literalExpr = new NullLiteral();
                break;
            case BOOLEAN:
                literalExpr = new BoolLiteral(value);
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                literalExpr = new IntLiteral(value, type);
                break;
            case LARGEINT:
                literalExpr = new LargeIntLiteral(value);
                break;
            case FLOAT:
            case DOUBLE:
                literalExpr = new FloatLiteral(value);
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                literalExpr = new DecimalLiteral(value);
                break;
            case CHAR:
            case VARCHAR:
            case HLL:
            case STRING:
                literalExpr = new StringLiteral(value);
                literalExpr.setType(type);
                break;
            case JSONB:
                literalExpr = new JsonLiteral(value);
                break;
            case DATE:
            case DATETIME:
            case DATEV2:
            case DATETIMEV2:
                literalExpr = new DateLiteral(value, type);
                break;
            case IPV4:
                literalExpr = new IPv4Literal(value);
                break;
            case IPV6:
                literalExpr = new IPv6Literal(value);
                break;
            default:
                throw new AnalysisException("Type[" + type.toSql() + "] not supported.");
        }

        Preconditions.checkNotNull(literalExpr);
        return literalExpr;
    }

    public static String getStringLiteralForComplexType(Expr v, FormatOptions options) {
        if (!(v instanceof NullLiteral) && v.getType().isScalarType()
                && (Type.getNumericTypes().contains((ScalarType) v.getActualScalarType(v.getType()))
                || v.getType() == Type.BOOLEAN)) {
            return v.getStringValueInFe(options);
        } else if (v.getType().isComplexType()) {
            // these type should also call getStringValueInFe which should handle special case for itself
            return v.getStringValueInFe(options);
        } else {
            return v.getStringValueForArray(options);
        }
    }

    public static String getStringLiteralForStreamLoad(Expr v, FormatOptions options) {
        if (!(v instanceof NullLiteral) && v.getType().isScalarType()
                && (Type.getNumericTypes().contains((ScalarType) v.getActualScalarType(v.getType()))
                || v.getType() == Type.BOOLEAN)) {
            return v.getStringValueInFe(options);
        } else if (v.getType().isComplexType()) {
            // these type should also call getStringValueInFe which should handle special case for itself
            return v.getStringValueForStreamLoad(options);
        } else {
            return v.getStringValueForArray(options);
        }
    }

    /**
     * Init LiteralExpr's Type information
     * only use in rewrite alias function
     * @param expr
     * @return
     * @throws AnalysisException
     */
    public static LiteralExpr init(LiteralExpr expr) throws AnalysisException {
        Preconditions.checkArgument(expr.getType().equals(Type.INVALID));
        String value = expr.getStringValue();
        LiteralExpr literalExpr = null;
        if (expr instanceof NullLiteral) {
            literalExpr = new NullLiteral();
        } else if (expr instanceof BoolLiteral) {
            literalExpr = new BoolLiteral(value);
        } else if (expr instanceof IntLiteral) {
            literalExpr = new IntLiteral(Long.parseLong(value));
        } else if (expr instanceof LargeIntLiteral) {
            literalExpr = new LargeIntLiteral(value);
        } else if (expr instanceof FloatLiteral) {
            literalExpr = new FloatLiteral(value);
        } else if (expr instanceof DecimalLiteral) {
            literalExpr = new DecimalLiteral(value);
        } else if (expr instanceof StringLiteral) {
            literalExpr = new StringLiteral(value);
        } else if (expr instanceof JsonLiteral) {
            literalExpr = new JsonLiteral(value);
        } else if (expr instanceof DateLiteral) {
            literalExpr = new DateLiteral(value, expr.getType());
        } else {
            throw new AnalysisException("Type[" + expr.getType().toSql() + "] not supported.");
        }

        Preconditions.checkNotNull(literalExpr);
        return literalExpr;
    }

    public Expr convertTo(Type targetType) throws AnalysisException {
        Preconditions.checkArgument(!targetType.equals(Type.INVALID));
        if (this instanceof NullLiteral) {
            return NullLiteral.create(targetType);
        } else if (targetType.isBoolean()) {
            if (this instanceof StringLiteral || this instanceof JsonLiteral) {
                return new BoolLiteral(getStringValue());
            } else {
                if (getLongValue() != 0) {
                    return new BoolLiteral(true);
                } else {
                    return new BoolLiteral(false);
                }
            }
        } else if (targetType.isIntegerType()) {
            return new IntLiteral(getLongValue(), targetType);
        } else if (targetType.isLargeIntType()) {
            return new LargeIntLiteral(getStringValue());
        } else if (targetType.isFloatingPointType()) {
            return new FloatLiteral(getDoubleValue(), targetType);
        } else if (targetType.isDecimalV2() || targetType.isDecimalV3()) {
            DecimalLiteral literal = new DecimalLiteral(getStringValue(),
                    ((ScalarType) targetType).getScalarScale());
            literal.setType(targetType);
            return literal;
        } else if (targetType.isStringType()) {
            return new StringLiteral(getStringValue());
        } else if (targetType.isDateType()) {
            return new StringLiteral(getStringValue()).convertToDate(targetType);
        }
        return this;
    }

    public static LiteralExpr createInfinity(Type type, boolean isMax) throws AnalysisException {
        Preconditions.checkArgument(!type.equals(Type.INVALID));
        if (isMax) {
            return MaxLiteral.MAX_VALUE;
        }
        switch (type.getPrimitiveType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return IntLiteral.createMinValue(type);
            case LARGEINT:
                return LargeIntLiteral.createMinValue();
            case DATE:
            case DATETIME:
            case DATEV2:
            case DATETIMEV2:
                return DateLiteral.createMinValue(type);
            default:
                throw new AnalysisException("Invalid data type for creating infinity: " + type);
        }
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        // Literals require no analysis.
    }

    /*
     * return real value
     */
    public Object getRealValue() {
        // implemented: TINYINT/SMALLINT/INT/BIGINT/LARGEINT/DATE/DATETIME/CHAR/VARCHAR/BOOLEAN
        Preconditions.checkState(false, "should implement this in derived class. " + this.type.toSql());
        return null;
    }

    public abstract boolean isMinValue();

    // Only used by partition pruning and the derived class which can be used for pruning
    // must handle MaxLiteral.
    public abstract int compareLiteral(LiteralExpr expr);

    @Override
    public int compareTo(LiteralExpr literalExpr) {
        return compareLiteral(literalExpr);
    }

    // Returns the string representation of the literal's value. Used when passing
    // literal values to the metastore rather than to Palo backends. This is similar to
    // the toSql() method, but does not perform any formatting of the string values. Neither
    // method unescapes string values.
    @Override
    public abstract String getStringValue();

    public String getStringValueInFe(FormatOptions options) {
        return getStringValue();
    }

    @Override
    public abstract String getStringValueForArray(FormatOptions options);

    public long getLongValue() {
        return 0;
    }

    public double getDoubleValue() {
        return 0;
    }

    public ByteBuffer getHashValue(PrimitiveType type) {
        String value = getStringValue();
        ByteBuffer buffer;
        try {
            buffer = ByteBuffer.wrap(value.getBytes("UTF-8"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return buffer;
    }

    @Override
    public String toDigestImpl() {
        return " ? ";
    }

    // Swaps the sign of numeric literals.
    // Throws for non-numeric literals.
    public void swapSign() throws NotImplementedException {
        throw new NotImplementedException("swapSign() only implemented for numeric" + "literals");
    }

    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LiteralExpr)) {
            return false;
        }
        //TODO chenhao16, call super.equals()
        if ((obj instanceof StringLiteral && !(this instanceof StringLiteral))
                || (this instanceof StringLiteral && !(obj instanceof StringLiteral))
                || (obj instanceof DecimalLiteral && !(this instanceof DecimalLiteral))
                || (this instanceof DecimalLiteral && !(obj instanceof DecimalLiteral))) {
            return false;
        }
        return this.compareLiteral(((LiteralExpr) obj)) == 0;
    }

    @Override
    public boolean isNullable() {
        // TODO: use base class's isNullLiteral() to replace this
        return this instanceof NullLiteral;
    }

    @Override
    public String toString() {
        return getStringValue();
    }

    public static LiteralExpr getLiteralByMysqlType(int mysqlType, boolean isUnsigned) throws AnalysisException {
        LiteralExpr literalExpr = null;

        // If this is an unsigned numeric type, we convert it by using larger data types. For example, we can use
        // small int to represent unsigned tiny int (0-255), big int to represent unsigned ints (0-2 ^ 32-1),
        // and so on.
        switch (mysqlType & MysqlColType.MYSQL_CODE_MASK) {
            case 1: // MYSQL_TYPE_TINY
                literalExpr = LiteralExpr.create("0", !isUnsigned ? Type.TINYINT : Type.SMALLINT);
                break;
            case 2: // MYSQL_TYPE_SHORT
                literalExpr = LiteralExpr.create("0", !isUnsigned ? Type.SMALLINT : Type.INT);
                break;
            case 3: // MYSQL_TYPE_LONG
                literalExpr = LiteralExpr.create("0", !isUnsigned ? Type.INT : Type.BIGINT);
                break;
            case 8: // MYSQL_TYPE_LONGLONG
                literalExpr = LiteralExpr.create("0", !isUnsigned ? Type.BIGINT : Type.LARGEINT);
                break;
            case 4: // MYSQL_TYPE_FLOAT
                literalExpr = LiteralExpr.create("0", Type.FLOAT);
                break;
            case 5: // MYSQL_TYPE_DOUBLE
                literalExpr = LiteralExpr.create("0", Type.DOUBLE);
                literalExpr.setType(Type.DOUBLE);
                break;
            case 0: // MYSQL_TYPE_DECIMAL
            case 246: // MYSQL_TYPE_NEWDECIMAL
                literalExpr = LiteralExpr.create("0", Type.DECIMAL32);
                break;
            case 11: // MYSQL_TYPE_TIME
                literalExpr = LiteralExpr.create("", Type.TIME);
                break;
            case 10: // MYSQL_TYPE_DATE
                literalExpr = LiteralExpr.create("1970-01-01", Type.DATE);
                break;
            case 12: // MYSQL_TYPE_DATETIME
            case 7: // MYSQL_TYPE_TIMESTAMP
            case 17: // MYSQL_TYPE_TIMESTAMP2
                literalExpr = LiteralExpr.create("1970-01-01 00:00:00", Type.DATETIME);
                break;
            case 254: // MYSQL_TYPE_STRING
            case 253: // MYSQL_TYPE_VAR_STRING
                literalExpr = LiteralExpr.create("", Type.STRING);
                break;
            case 15: // MYSQL_TYPE_VARCHAR
                literalExpr = LiteralExpr.create("", Type.VARCHAR);
                break;
            default:
                throw new AnalysisException("Unsupported MySQL type: " + mysqlType);
        }
        return literalExpr;
    }

    @Override
    public String getExprName() {
        if (!this.exprName.isPresent()) {
            this.exprName = Optional.of("literal");
        }
        return this.exprName.get();
    }

    // Port from mysql get_param_length
    public static int getParmLen(ByteBuffer data) {
        int maxLen = data.remaining();
        if (maxLen < 1) {
            return 0;
        }
        // get and advance 1 byte
        int len = MysqlProto.readInt1(data);
        if (len == 252) {
            if (maxLen < 3) {
                return 0;
            }
            // get and advance 2 bytes
            return MysqlProto.readInt2(data);
        } else if (len == 253) {
            if (maxLen < 4) {
                return 0;
            }
            // get and advance 3 bytes
            return MysqlProto.readInt3(data);
        } else if (len == 254) {
            /*
            In our client-server protocol all numbers bigger than 2^24
            stored as 8 bytes with uint8korr. Here we always know that
            parameter length is less than 2^4 so we don't look at the second
            4 bytes. But still we need to obey the protocol hence 9 in the
            assignment below.
            */
            if (maxLen < 9) {
                return 0;
            }
            len = MysqlProto.readInt4(data);
            MysqlProto.readFixedString(data, 4);
            return len;
        } else if (len == 255) {
            return 0;
        } else {
            return len;
        }
    }

    @Override
    public boolean matchExprs(List<Expr> exprs, SelectStmt stmt, boolean ignoreAlias, TupleDescriptor tuple) {
        return true;
    }

    /** whether is ZERO value **/
    public boolean isZero() {
        boolean isZero = false;
        switch (type.getPrimitiveType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case LARGEINT:
                isZero = this.getLongValue() == 0;
                break;
            case FLOAT:
            case DOUBLE:
                isZero = this.getDoubleValue() == 0.0f;
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                isZero = Objects.equals(((DecimalLiteral) this).getValue(), BigDecimal.ZERO);
                break;
            default:
        }
        return isZero;
    }

    public static LiteralExpr getLiteralExprFromThrift(TExprNode node) throws AnalysisException {
        TExprNodeType type = node.node_type;
        switch (type) {
            case NULL_LITERAL: return new NullLiteral();
            case BOOL_LITERAL: return new BoolLiteral(node.bool_literal.value);
            case INT_LITERAL: return new IntLiteral(node.int_literal.value);
            case LARGE_INT_LITERAL: return new LargeIntLiteral(node.large_int_literal.value);
            case FLOAT_LITERAL: return new FloatLiteral(node.float_literal.value);
            case DECIMAL_LITERAL: return new DecimalLiteral(node.decimal_literal.value);
            case STRING_LITERAL: return new StringLiteral(node.string_literal.value);
            case JSON_LITERAL: return new JsonLiteral(node.json_literal.value);
            case DATE_LITERAL: return new DateLiteral(node.date_literal.value);
            case IPV4_LITERAL: return new IPv4Literal(node.ipv4_literal.value);
            case IPV6_LITERAL: return new IPv6Literal(node.ipv6_literal.value);
            default: throw new AnalysisException("Wrong type from thrift;");
        }
    }

    // Parse from binary data, the format follows mysql binary protocal
    // see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html.
    // Return next offset
    public void setupParamFromBinary(ByteBuffer data, boolean isUnsigned) {
        Preconditions.checkState(false,
                "should implement this in derived class. " + this.type.toSql());
    }
}
