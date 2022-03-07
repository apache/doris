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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.qe.VariableVarConverters;
import org.apache.doris.thrift.TBoolLiteral;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TFloatLiteral;
import org.apache.doris.thrift.TIntLiteral;
import org.apache.doris.thrift.TStringLiteral;

import com.google.common.base.Strings;

import java.util.Objects;

// System variable
// Converted to StringLiteral in analyze, if this variable is not exist, throw AnalysisException.
public class SysVariableDesc extends Expr {
    private String name;
    private SetType setType;
    private boolean boolValue;
    private long intValue;
    private double floatValue;
    private String strValue;

    private LiteralExpr literalExpr;

    public SysVariableDesc(String name) {
        this(name, SetType.SESSION);
    }

    public SysVariableDesc(String name, SetType setType) {
        this.name = name;
        this.setType = setType;
    }

    protected SysVariableDesc(SysVariableDesc other) {
        super(other);
        name = other.name;
        setType = other.setType;
        boolValue = other.boolValue;
        intValue = other.intValue;
        floatValue = other.floatValue;
        strValue = other.strValue;
    }

    @Override
    public Expr clone() {
        return new SysVariableDesc(this);
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        VariableMgr.fillValue(analyzer.getContext().getSessionVariable(), this);
        if (!Strings.isNullOrEmpty(name) && VariableVarConverters.hasConverter(name)) {
            setType(Type.VARCHAR);
            try {
                setStringValue(VariableVarConverters.decode(name, intValue));
            } catch (DdlException e) {
                ErrorReport.reportAnalysisException(e.getMessage());
            }
        }
    }

    public String getName() {
        return name;
    }

    public SetType getSetType() {
        return setType;
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
    public Expr getResultValue() throws AnalysisException {
        if (!Strings.isNullOrEmpty(name) && VariableVarConverters.hasConverter(name)) {
            // Return the string type here so that it can correctly match the subsequent function signature.
            // And we also set `beConverted` to session variable name in StringLiteral, so that it can be cast back
            // to Integer when returning value.
            try {
                StringLiteral s = new StringLiteral(VariableVarConverters.decode(name, intValue));
                s.setBeConverted(name);
                return s;
            } catch (DdlException e) {
                throw new AnalysisException(e.getMessage());
            }
        }
        return super.getResultValue();
    }

    @Override
    protected boolean isConstantImpl() {
        return true;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        switch (type.getPrimitiveType()) {
            case BOOLEAN:
                msg.node_type = TExprNodeType.BOOL_LITERAL;
                msg.bool_literal = new TBoolLiteral(boolValue);
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                msg.node_type = TExprNodeType.INT_LITERAL;
                msg.int_literal = new TIntLiteral(intValue);
                break;
            case FLOAT:
            case DOUBLE:
                msg.node_type = TExprNodeType.FLOAT_LITERAL;
                msg.float_literal = new TFloatLiteral(floatValue);
                break;
            default:
                msg.node_type = TExprNodeType.STRING_LITERAL;
                msg.string_literal = new TStringLiteral(strValue);
        }
    }

    @Override
    public String toSqlImpl() {
        StringBuilder sb = new StringBuilder("@@");
        if (setType == SetType.GLOBAL) {
            sb.append("GLOBAL.");
        }
        sb.append(name);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SysVariableDesc)) {
            return false;
        }
        if (!name.equals(((SysVariableDesc) obj).getName())) {
            return false;
        }
        if (!setType.equals(((SysVariableDesc) obj).getSetType())) {
            return false;
        }

        return Objects.equals(literalExpr, ((SysVariableDesc) obj).getLiteralExpr());
    }
}
