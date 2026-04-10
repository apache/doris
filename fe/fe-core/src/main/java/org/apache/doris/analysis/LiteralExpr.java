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

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

public abstract class LiteralExpr extends Expr implements Comparable<LiteralExpr> {
    public LiteralExpr() {
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
                literalExpr = DecimalLiteralUtils.create(value);
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
            case TIMESTAMPTZ:
                literalExpr = DateLiteralUtils.createDateLiteral(value, type);
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
            case TIMESTAMPTZ:
                return DateLiteral.createMinValue(type);
            default:
                throw new AnalysisException("Invalid data type for creating infinity: " + type);
        }
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
    public String toString() {
        return getStringValue();
    }

}
