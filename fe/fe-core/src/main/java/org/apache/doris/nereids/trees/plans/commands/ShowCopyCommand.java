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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.proc.LoadProcDir;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

import java.util.List;

/**
 * ShowCopyCommand
 */
public class ShowCopyCommand extends ShowLoadCommand {

    public ShowCopyCommand(String catalog, String db, List<OrderKey> orderByElements,
                           Expression where, String likePattern, long limit, long offset) {
        super(catalog, db, orderByElements, where, likePattern, limit, offset, false);
    }

    @Override
    protected void analyzeSubExpr(Expression expr) throws AnalysisException {
        if (expr == null) {
            return;
        }
        boolean valid = isWhereClauseValid(expr);
        if (!valid) {
            throw new AnalysisException("Where clause should looks like: LABEL = \"your_load_label\","
                + " or LABEL LIKE \"matcher\", " + " or STATE = \"PENDING|ETL|LOADING|FINISHED|CANCELLED\", "
                + " or Id = \"your_query_id\", or ID LIKE \"matcher\", "
                + " or TableName = \"your_table_name\", or TableName LIKE \"matcher\", "
                + " or Files = \"your_file_name\", or FILES LIKE \"matcher\", "
                + " or compound predicate with operator AND");
        }
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

        if (!(expr.child(0) instanceof Slot)) {
            return false;
        }

        String leftKey = ((Slot) expr.child(0)).getName();

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
}
