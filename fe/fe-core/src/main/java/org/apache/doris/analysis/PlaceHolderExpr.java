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

import org.apache.doris.catalog.MysqlColType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.thrift.TExprNode;

import com.google.common.base.Preconditions;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

// PlaceHolderExpr is a reference class point to real LiteralExpr
public class PlaceHolderExpr extends LiteralExpr {
    private LiteralExpr lExpr;
    int mysqlTypeCode = -1;

    public PlaceHolderExpr() {
        type = Type.UNSUPPORTED;
    }

    public void setTypeCode(int mysqlTypeCode) {
        this.mysqlTypeCode = mysqlTypeCode;
    }

    protected PlaceHolderExpr(LiteralExpr literal) {
        this.lExpr = literal;
        this.type = literal.getType();
    }

    protected PlaceHolderExpr(PlaceHolderExpr other) {
        this.lExpr = other.lExpr;
        this.type = other.type;
    }

    public void setLiteral(LiteralExpr literal) {
        this.lExpr = literal;
        this.type = literal.getType();
    }

    public LiteralExpr getLiteral() {
        return lExpr;
    }

    @Override
    protected void analysisDone() {
        if (lExpr != null && !lExpr.isAnalyzed) {
            lExpr.analysisDone();
        }
        if (!isAnalyzed) {
            super.analysisDone();
        }
    }

    public LiteralExpr createLiteralFromType() throws AnalysisException {
        Preconditions.checkState(mysqlTypeCode > 0);
        return LiteralExpr.getLiteralByMysqlType(mysqlTypeCode, isUnsigned());
    }

    public static PlaceHolderExpr create(String value, Type type) throws AnalysisException {
        Preconditions.checkArgument(!type.equals(Type.INVALID));
        return new PlaceHolderExpr(LiteralExpr.create(value, type));
    }

    @Override
    protected void toThrift(TExprNode msg) {
        lExpr.toThrift(msg);
    }

    /*
     * return real value
     */
    public Object getRealValue() {
        // implemented: TINYINT/SMALLINT/INT/BIGINT/LARGEINT/DATE/DATETIME/CHAR/VARCHAR/BOOLEAN
        Preconditions.checkState(false, "not implement this in derived class. " + this.type.toSql());
        return null;
    }

    @Override
    public boolean isMinValue() {
        return lExpr.isMinValue();
    }

    public boolean isUnsigned() {
        return MysqlColType.isUnsigned(mysqlTypeCode);
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        return lExpr.compareLiteral(expr);
    }

    @Override
    public int compareTo(LiteralExpr literalExpr) {
        return compareLiteral(literalExpr);
    }

    public String getStringValue() {
        if (lExpr == null) {
            return "";
        }
        return lExpr.getStringValue();
    }

    public long getLongValue() {
        return lExpr.getLongValue();
    }

    public double getDoubleValue() {
        return lExpr.getDoubleValue();
    }

    public ByteBuffer getHashValue(PrimitiveType colType) {
        if (colType != type.getPrimitiveType()) {
            try {
                LiteralExpr castLiteral = LiteralExpr.create(getStringValue(), Type.fromPrimitiveType(colType));
                return castLiteral.getHashValue(colType);
            } catch (AnalysisException e) {
                // Could not reach this position
                Preconditions.checkState(false);
            }
        }
        return lExpr.getHashValue(colType);
    }

    @Override
    public String toDigestImpl() {
        return "?";
    }

    // Swaps the sign of numeric literals.
    // Throws for non-numeric literals.
    public void swapSign() throws NotImplementedException {
        Preconditions.checkState(false, "should not implement this in derived class. " + this.type.toSql());
    }

    @Override
    public boolean supportSerializable() {
        return false;
    }

    public void readFields(DataInput in) throws IOException {
        Preconditions.checkState(false, "should not implement this in derived class. " + this.type.toSql());
    }

    @Override
    public boolean isNullable() {
        return this.lExpr instanceof NullLiteral;
    }

    @Override
    public Expr clone() {
        // Should not clone, since it's a reference class
        return this;
    }

    @Override
    public String toSqlImpl() {
        if (this.lExpr == null) {
            return "?";
        }
        return "_placeholder_(" + this.lExpr.toSqlImpl() + ")";
    }

    // @Override
    public Expr reset() {
        this.lExpr = null;
        this.type = Type.UNSUPPORTED;
        return this;
    }

    @Override
    public String getStringValueForArray(FormatOptions options) {
        return options.getNestedStringWrapper() + getStringValue() + options.getNestedStringWrapper();
    }

    public void setupParamFromBinary(ByteBuffer data, boolean isUnsigned) {
        lExpr.setupParamFromBinary(data, isUnsigned);
    }
}
