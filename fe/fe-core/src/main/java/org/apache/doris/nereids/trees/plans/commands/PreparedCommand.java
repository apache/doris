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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.PlaceholderExpr;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Prepared Statement
 */
public class PreparedCommand extends LogicalPlanAdapter {
    protected List<PlaceholderExpr> params;
    private final StatementBase inner;

    private String stmtName;

    public PreparedCommand(String name, StatementBase originStmt, List<PlaceholderExpr> placeholders) {
        super(null, null);
        this.inner = originStmt;
        this.params = placeholders;
        this.stmtName = name;
    }

    public String getName() {
        return stmtName;
    }

    public void setName(String name) {
        this.stmtName = name;
    }

    public List<PlaceholderExpr> params() {
        return params;
    }

    public int getParamLen() {
        if (params == null) {
            return 0;
        }
        return params.size();
    }

    public void setParams(List<PlaceholderExpr> params) {
        this.params = params;
    }

    public StatementBase getInnerStmt() {
        return inner;
    }

    /**
    *  assign real value to placeholders and return the assigned statement
    */
    public StatementBase assignValues(List<Expression> values) {
        if (params == null) {
            return inner;
        }
        Preconditions.checkArgument(values.size() == params.size(), "Invalid arguments size");
        for (int i = 0; i < params.size(); i++) {
            params.get(i).setExpr(values.get(i));
        }
        return inner;
    }

    /**
     * return the labels of paramters
     */
    public List<String> getLabels() {
        List<String> labels = new ArrayList<>();
        if (params == null) {
            return labels;
        }
        for (PlaceholderExpr parameter : params) {
            labels.add("$" + parameter.getExprId());
        }
        return labels;
    }

    @Override
    public String toSql() {
        return "PREPARE " + stmtName + " FROM " + inner.toSql();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return inner.getRedirectStatus();
    }
}
