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

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

// PlaceHolderExpr is a reference class point to real LiteralExpr
public class PlaceHolderExpr extends LiteralExpr {
    private LiteralExpr lExpr;
    int mysqlTypeCode = -1;

    public PlaceHolderExpr() {
        type = Type.UNSUPPORTED;
        this.nullable = false;
    }

    protected PlaceHolderExpr(LiteralExpr literal) {
        this.lExpr = literal;
        this.type = literal.getType();
        this.nullable = false;
    }

    public LiteralExpr getLiteral() {
        return lExpr;
    }

    public static PlaceHolderExpr create(String value, Type type) throws AnalysisException {
        Preconditions.checkArgument(!type.equals(Type.INVALID));
        return new PlaceHolderExpr(LiteralExpr.create(value, type));
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
    public Expr clone() {
        // Should not clone, since it's a reference class
        return this;
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitPlaceHolderExpr(this, context);
    }


}
