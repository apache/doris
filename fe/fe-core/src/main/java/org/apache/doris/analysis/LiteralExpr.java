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

package org.apache.doris.analysis;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.NotImplementedException;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

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
                literalExpr = new DecimalLiteral(value);
                break;
            case CHAR:
            case VARCHAR:
            case HLL:
            case STRING:
                literalExpr = new StringLiteral(value);
                literalExpr.setType(type);
                break;
            case DATE:
            case DATETIME:
                literalExpr = new DateLiteral(value, type);
                break;
            default:
                throw new AnalysisException("Type[" + type.toSql() + "] not supported.");
        }

        Preconditions.checkNotNull(literalExpr);
        return literalExpr;
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
        } else if (expr instanceof DateLiteral) {
            literalExpr = new DateLiteral(value, expr.getType());
        } else {
            throw new AnalysisException("Type[" + expr.getType().toSql() + "] not supported.");
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
    public String getStringValue() {
        return null;
    }

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

    // Swaps the sign of numeric literals.
    // Throws for non-numeric literals.
    public void swapSign() throws NotImplementedException {
        throw new NotImplementedException("swapSign() only implemented for numeric" + "literals");
    }

    @Override
    public boolean supportSerializable() {
        return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
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
        return this instanceof NullLiteral;
    }
}

