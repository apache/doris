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

import java.math.BigDecimal;
import java.util.Objects;

// Variable expr: including the system variable and user define variable.
// Converted to StringLiteral in analyze, if this variable is not exist, throw AnalysisException.
public class VariableExpr extends Expr {
    private String name;
    private SetType setType;
    private boolean isNull;
    private boolean boolValue;
    private long intValue;
    private double floatValue;
    private BigDecimal decimalValue;
    private String strValue;

    private LiteralExpr literalExpr;

    public VariableExpr(String name) {
        this(name, SetType.SESSION);
    }

    public VariableExpr(String name, SetType setType) {
        this.name = name;
        this.setType = setType;
        this.nullable = false;
    }

    protected VariableExpr(VariableExpr other) {
        super(other);
        name = other.name;
        setType = other.setType;
        isNull = other.isNull;
        boolValue = other.boolValue;
        intValue = other.intValue;
        floatValue = other.floatValue;
        strValue = other.strValue;
    }

    @Override
    public Expr clone() {
        return new VariableExpr(this);
    }

    public String getName() {
        return name;
    }

    public SetType getSetType() {
        return setType;
    }

    public void setIsNull() {
        isNull = true;
    }

    public boolean isNull() {
        return isNull;
    }

    public boolean getBoolValue() {
        return boolValue;
    }

    public long getIntValue() {
        return intValue;
    }

    public double getFloatValue() {
        return floatValue;
    }

    public String getStrValue() {
        return strValue;
    }

    public void setBoolValue(boolean value) {
        this.boolValue = value;
        this.literalExpr = new BoolLiteral(value);
    }

    public void setIntValue(long value) {
        this.intValue = value;
        this.literalExpr = new IntLiteral(value);
    }

    public void setFloatValue(double value) {
        this.floatValue = value;
        this.literalExpr = new FloatLiteral(value);
    }

    public void setStringValue(String value) {
        this.strValue = value;
        this.literalExpr = new StringLiteral(value);
    }

    public Expr getLiteralExpr() {
        return this.literalExpr;
    }

    @Override
    protected boolean isConstantImpl() {
        return true;
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitVariableExpr(this, context);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (getSetType() == SetType.USER) {
            sb.append("@");
        } else {
            sb.append("@@");
            if (getSetType() == SetType.GLOBAL) {
                sb.append("GLOBAL.");
            }
        }
        sb.append(getName());
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof VariableExpr)) {
            return false;
        }
        if (!name.equals(((VariableExpr) obj).getName())) {
            return false;
        }
        if (!setType.equals(((VariableExpr) obj).getSetType())) {
            return false;
        }

        return Objects.equals(literalExpr, ((VariableExpr) obj).getLiteralExpr());
    }
}
