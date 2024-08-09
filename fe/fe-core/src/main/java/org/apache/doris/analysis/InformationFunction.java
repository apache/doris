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
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TInfoFunc;

import com.google.gson.annotations.SerializedName;

public class InformationFunction extends Expr {
    @SerializedName("ft")
    private String funcType;
    private long intValue;
    private String strValue;

    private InformationFunction() {
        // only for serde
    }

    // First child is the comparison expr which should be in [lowerBound, upperBound].
    public InformationFunction(String funcType) {
        this.funcType = funcType;
    }

    protected InformationFunction(InformationFunction other) {
        super(other);
        funcType = other.funcType;
        intValue = other.intValue;
        strValue = other.strValue;
    }

    public String getStrValue() {
        return strValue;
    }

    public String getIntValue() {
        return String.valueOf(intValue);
    }

    public String getFuncType() {
        return funcType;
    }

    @Override
    public Expr clone() {
        return new InformationFunction(this);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        if (funcType.equalsIgnoreCase("DATABASE") || funcType.equalsIgnoreCase("SCHEMA")) {
            type = Type.VARCHAR;
            strValue = ClusterNamespace.getNameFromFullName(analyzer.getDefaultDb());
        } else if (funcType.equalsIgnoreCase("USER")) {
            type = Type.VARCHAR;
            strValue = ConnectContext.get().getUserIdentity().toString();
        } else if (funcType.equalsIgnoreCase("CURRENT_USER")) {
            type = Type.VARCHAR;
            strValue = ConnectContext.get().getCurrentUserIdentity().toString();
        } else if (funcType.equalsIgnoreCase("CONNECTION_ID")) {
            type = Type.BIGINT;
            intValue = analyzer.getConnectId();
            strValue = "";
        } else if (funcType.equalsIgnoreCase("CURRENT_CATALOG")) {
            type = Type.VARCHAR;
            strValue = ConnectContext.get().getDefaultCatalog();
        }
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.INFO_FUNC;
        msg.info_func = new TInfoFunc(intValue, strValue);
    }

    @Override
    public String toSqlImpl() {
        return funcType + "()";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof InformationFunction)) {
            return false;
        }

        if (!funcType.equals(((InformationFunction) obj).getFuncType())) {
            return false;
        }

        if (type.equals(Type.VARCHAR)) {
            if (!strValue.equals(((InformationFunction) obj).getStrValue())) {
                return false;
            }
        } else if (type.equals(Type.BIGINT)) {
            if (intValue != Integer.parseInt(((InformationFunction) obj).getIntValue())) {
                return false;
            }
        }
        return true;
    }
}
