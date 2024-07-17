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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/NullLiteral.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class NullLiteral extends LiteralExpr {

    private static final LiteralExpr INT_EXPR;

    static {
        try {
            INT_EXPR =  new IntLiteral("0", Type.INT);
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
    }

    public NullLiteral() {
        type = Type.NULL;
    }

    public static NullLiteral create(Type type) {
        NullLiteral l = new NullLiteral();
        l.type = type;
        return l;
    }

    protected NullLiteral(NullLiteral other) {
        super(other);
    }

    @Override
    protected void resetAnalysisState() {
        super.resetAnalysisState();
        type = Type.NULL;
    }

    @Override
    public Expr clone() {
        return new NullLiteral(this);
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return obj instanceof NullLiteral;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof PlaceHolderExpr) {
            return this.compareLiteral(((PlaceHolderExpr) expr).getLiteral());
        }
        if (expr instanceof NullLiteral) {
            return 0;
        }
        return -1;
    }

    @Override
    public String toSqlImpl() {
        return getStringValue();
    }

    @Override
    public String getStringValue() {
        return "NULL";
    }

    @Override
    public String getStringValueInFe(FormatOptions options) {
        return null;
    }

    // the null value inside an array is represented as "null", for exampe:
    // [null, null]. Not same as other primitive type to represent as \N.
    @Override
    public String getStringValueForArray(FormatOptions options) {
        return options.getNullFormat();
    }

    @Override
    public long getLongValue() {
        return 0;
    }

    @Override
    public double getDoubleValue() {
        return 0.0;
    }

    // for distribution prune
    // https://dev.mysql.com/doc/refman/5.7/en/partitioning-handling-nulls.html
    // DATE DATETIME: MIN_VALUE
    // CHAR VARCHAR: ""
    @Override
    public ByteBuffer getHashValue(PrimitiveType type) {
        return INT_EXPR.getHashValue(PrimitiveType.INT);
    }

    @Override
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        Preconditions.checkState(targetType.isValid());
        if (!type.equals(targetType)) {
            NullLiteral nullLiteral = new NullLiteral(this);
            nullLiteral.setType(targetType);
            return nullLiteral;
        }
        return this;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.NULL_LITERAL;
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
    }

    public static NullLiteral read(DataInput in) throws IOException {
        NullLiteral literal = new NullLiteral();
        literal.readFields(in);
        return literal;
    }
}
