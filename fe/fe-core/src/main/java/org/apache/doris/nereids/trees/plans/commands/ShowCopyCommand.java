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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.proc.LoadProcDir;
import org.apache.doris.load.LoadJob.JobState;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * ShowCopyCommand
 */
public class ShowCopyCommand extends ShowLoadCommand {

    private String dbName;
    private Expression whereClause;

    public ShowCopyCommand(String dbName, List<OrderKey> orderByElements,
                           Expression where, long limit, long offset) {
        super(where, orderByElements, limit, offset, dbName, false);
        this.dbName = dbName;
        this.whereClause = where;
    }

    @Override
    protected boolean validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        // analyze where clause if not null
        if (whereClause != null) {
            if (whereClause instanceof CompoundPredicate) {
                return analyzeCompoundPredicate(whereClause);
            } else {
                return analyzeSubPredicate(whereClause);
            }
        }
        return true;
    }

    private boolean analyzeSubPredicate(Expression expr) throws AnalysisException {
        if (expr == null) {
            return true;
        }
        return isWhereClauseValid(expr);
    }

    private boolean isWhereClauseValid(Expression expr) {
        boolean hasLabel = false;
        boolean hasState = false;
        boolean hasCopyId = false;
        boolean hasTableName = false;
        boolean hasFile = false;

        if (!((expr instanceof EqualTo) || (expr instanceof Like))) {
            return false;
        }

        if (!(expr.child(0) instanceof UnboundSlot)) {
            return false;
        }

        String leftKey = ((UnboundSlot) expr.child(0)).getName();

        if (leftKey.equalsIgnoreCase("label")) {
            hasLabel = true;
        } else if (leftKey.equalsIgnoreCase("state")) {
            hasState = true;
        } else if (leftKey.equalsIgnoreCase("id")) {
            hasCopyId = true;
        } else if (leftKey.equalsIgnoreCase("TableName")) {
            hasTableName = true;
        } else if (leftKey.equalsIgnoreCase("files")) {
            hasFile = true;
        } else {
            return false;
        }

        if (hasState && !(expr instanceof EqualTo)) {
            return false;
        }

        if (hasLabel && expr instanceof EqualTo) {
            isAccurateMatch = true;
        }

        if (hasCopyId && expr instanceof EqualTo) {
            isCopyIdAccurateMatch = true;
        }

        if (hasTableName && expr instanceof EqualTo) {
            isTableNameAccurateMatch = true;
        }

        if (hasFile && expr instanceof EqualTo) {
            isFilesAccurateMatch = true;
        }

        // right child
        if (!(expr.child(1) instanceof StringLiteral)) {
            return false;
        }

        String value = ((StringLiteral) expr.child(1)).getStringValue();
        if (Strings.isNullOrEmpty(value)) {
            return false;
        }

        if (hasLabel && !isAccurateMatch && !value.contains("%")) {
            value = "%" + value + "%";
        }
        if (hasCopyId && !isCopyIdAccurateMatch && !value.contains("%")) {
            value = "%" + value + "%";
        }
        if (hasTableName && !isTableNameAccurateMatch && !value.contains("%")) {
            value = "%" + value + "%";
        }
        if (hasFile && !isFilesAccurateMatch && !value.contains("%")) {
            value = "%" + value + "%";
        }

        if (hasLabel) {
            labelValue = value;
        } else if (hasState) {
            stateValue = value.toUpperCase();

            try {
                JobState.valueOf(stateValue);
            } catch (Exception e) {
                return false;
            }
        } else if (hasCopyId) {
            copyIdValue = value;
        } else if (hasTableName) {
            tableNameValue = value;
        } else if (hasFile) {
            fileValue = value;
        }
        return true;
    }

    private boolean analyzeCompoundPredicate(Expression expr) throws AnalysisException {
        List<Expression> children = new ArrayList<>();
        analyzeCompoundPredicate(expr, children);
        Set<String> names = new HashSet<>();
        boolean result = true;
        for (Expression expression : children) {
            String name = ((UnboundSlot) expression.child(0)).getName();
            if (names.contains(name)) {
                throw new AnalysisException("column names on both sides of operator AND should be diffrent");
            }
            names.add(name);
            result = result && analyzeSubPredicate(expression);
            if (!result) {
                return result;
            }
        }
        return result;
    }

    private void analyzeCompoundPredicate(Expression expr, List<Expression> exprs) throws AnalysisException {
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate cp = (CompoundPredicate) expr;
            if (!(cp instanceof And)) {
                throw new AnalysisException("Only allow compound predicate with operator AND");
            }
            analyzeCompoundPredicate(expr.child(0), exprs);
            analyzeCompoundPredicate(expr.child(1), exprs);
        } else {
            exprs.add(expr);
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCopyCommand(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : LoadProcDir.COPY_TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
