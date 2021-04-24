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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TBoolLiteral;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

public class BoolLiteral extends LiteralExpr {
    private boolean value;
    
    private BoolLiteral() {
    }

    public BoolLiteral(boolean value) {
        this.setValue(value);
        type = Type.BOOLEAN;
    }

    public BoolLiteral(String value) throws AnalysisException {
        this.type = Type.BOOLEAN;
        if (value.trim().toLowerCase().equals("true") || value.trim().equals("1")) {
            this.setValue(true);
        } else if (value.trim().toLowerCase().equals("false") || value.trim().equals("0")) {
            this.setValue(false);
        } else {
            throw new AnalysisException("Invalid BOOLEAN literal: " + value);
        }
    }

    protected BoolLiteral(BoolLiteral other) {
        super(other);
        this.setValue(other.value);
    }

    @Override
    public Expr clone() {
        return new BoolLiteral(this);
    }

    private void setValue(boolean value) {
        this.value = value;
        this.selectivity = value ? 1 : 0;
    }

    public boolean getValue() {
        return value;
    }

    @Override
    public Object getRealValue() {
        return getValue();
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof NullLiteral) {
            return 1;
        }
        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        return Long.signum(getLongValue() - expr.getLongValue());
    }

    @Override
    public String toSqlImpl() {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    public String getStringValue() {
        return value ? "1" : "0";
    }

    @Override
    public long getLongValue() {
        return value ? 1 : 0;
    }

    @Override
    public double getDoubleValue() {
        return value ? 1.0 : 0.0;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.BOOL_LITERAL;
        msg.bool_literal = new TBoolLiteral(value);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeBoolean(value);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        this.setValue(in.readBoolean());
    }
    
    public static BoolLiteral read(DataInput in) throws IOException {
        BoolLiteral literal = new BoolLiteral();
        literal.readFields(in);
        return literal;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Boolean.hashCode(value);
    }
}
