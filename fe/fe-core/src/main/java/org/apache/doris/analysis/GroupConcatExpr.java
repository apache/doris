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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExprNode;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class GroupConcatExpr extends Expr {

    private final boolean distinct;

    private final String column;

    private final List<OrderByElement> orderByElements;

    private final String separator;

    public GroupConcatExpr(boolean distinct, String column, List<OrderByElement> orderByElements, String separator) {
        this.distinct = distinct;
        this.column = column;
        this.orderByElements = orderByElements == null ? new ArrayList<>() : orderByElements;
        if (StringUtils.isNotEmpty(separator)) {
            this.separator = separator;
        } else {
            this.separator = ",";
        }
    }

    protected GroupConcatExpr(GroupConcatExpr other) {
        super(other);
        this.distinct = other.distinct;
        this.column = other.column;
        this.orderByElements = other.orderByElements;
        this.separator = other.separator;
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        for (OrderByElement e : orderByElements) {
            if (e.getExpr().isConstant()) {
                throw new AnalysisException(
                        "Expressions in the ORDER BY clause must not be constant: "
                                + e.getExpr().toSql() + " (in " + toSql() + ")");
            }
        }
    }

    @Override
    protected String toSqlImpl() {
        StringBuilder sb = new StringBuilder("GROUP_CONCAT(");
        if (distinct) {
            sb.append("DISTINCT ");
        }
        sb.append(column);
        if (!orderByElements.isEmpty()) {
            List<String> orderByStrings = Lists.newArrayList();
            for (OrderByElement e : orderByElements) {
                orderByStrings.add(e.toSql());
            }
            sb.append(" ");
            sb.append("ORDER BY ").append(Joiner.on(", ").join(orderByStrings));
        }
        if (StringUtils.isNotBlank(separator)) {
            sb.append(" ");
            sb.append("SEPARATOR '").append(separator).append("'");
        }
        sb.append(")");
        return null;
    }

    @Override
    protected void toThrift(TExprNode msg) {

    }

    @Override
    public Expr clone() {
        return new GroupConcatExpr(this);
    }
}
