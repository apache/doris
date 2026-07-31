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

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

public abstract class LiteralExpr extends Expr implements Comparable<LiteralExpr> {
    public LiteralExpr() {
    }

    protected LiteralExpr(LiteralExpr other) {
        super(other);
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
