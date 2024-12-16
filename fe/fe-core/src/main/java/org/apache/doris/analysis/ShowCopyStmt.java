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

import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.proc.LoadProcDir;
import org.apache.doris.load.LoadJob.JobState;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// SHOW COPY STATUS statement used to get status of copy job.
//
// syntax:
//      SHOW COPY [FROM db] [LIKE mask]
public class ShowCopyStmt extends ShowLoadStmt implements NotFallbackInParser {
    private static final Logger LOG = LogManager.getLogger(ShowCopyStmt.class);

    public ShowCopyStmt(String db, Expr labelExpr, List<OrderByElement> orderByElements, LimitElement limitElement) {
        super(db, labelExpr, orderByElements, limitElement);
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
    protected void analyzeSubPredicate(Expr subExpr) throws AnalysisException {
        if (subExpr == null) {
            return;
        }

        boolean valid = true;
        boolean hasLabel = false;
        boolean hasState = false;
        boolean hasCopyId = false;
        boolean hasTableName = false;
        boolean hasFile = false;

        CHECK: {
            if (subExpr instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
                if (binaryPredicate.getOp() != Operator.EQ) {
                    valid = false;
                    break CHECK;
                }
            } else if (subExpr instanceof LikePredicate) {
                LikePredicate likePredicate = (LikePredicate) subExpr;
                if (likePredicate.getOp() != LikePredicate.Operator.LIKE) {
                    valid = false;
                    break CHECK;
                }
            } else {
                valid = false;
                break CHECK;
            }

            // left child
            if (!(subExpr.getChild(0) instanceof SlotRef)) {
                valid = false;
                break CHECK;
            }
            String leftKey = ((SlotRef) subExpr.getChild(0)).getColumnName();
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
                valid = false;
                break CHECK;
            }

            if (hasState && !(subExpr instanceof BinaryPredicate)) {
                valid = false;
                break CHECK;
            }

            if (hasLabel && subExpr instanceof BinaryPredicate) {
                isAccurateMatch = true;
            }

            if (hasCopyId && subExpr instanceof BinaryPredicate) {
                isCopyIdAccurateMatch = true;
            }

            if (hasTableName && subExpr instanceof BinaryPredicate) {
                isTableNameAccurateMatch = true;
            }

            if (hasFile && subExpr instanceof BinaryPredicate) {
                isFilesAccurateMatch = true;
            }

            // right child
            if (!(subExpr.getChild(1) instanceof StringLiteral)) {
                valid = false;
                break CHECK;
            }

            String value = ((StringLiteral) subExpr.getChild(1)).getStringValue();
            if (Strings.isNullOrEmpty(value)) {
                valid = false;
                break CHECK;
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
                    valid = false;
                    break CHECK;
                }
            } else if (hasCopyId) {
                copyIdValue = value;
            } else if (hasTableName) {
                tableNameValue = value;
            } else if (hasFile) {
                fileValue = value;
            }
        }

        if (!valid) {
            throw new AnalysisException("Where clause should looks like: LABEL = \"your_load_label\","
                    + " or LABEL LIKE \"matcher\", " + " or STATE = \"PENDING|ETL|LOADING|FINISHED|CANCELLED\", "
                    + " or Id = \"your_query_id\", or ID LIKE \"matcher\", "
                    + " or TableName = \"your_table_name\", or TableName LIKE \"matcher\", "
                    + " or Files = \"your_file_name\", or FILES LIKE \"matcher\", "
                    + " or compound predicate with operator AND");
        }
    }

    @Override
    protected void analyzeCompoundPredicate(Expr cp) throws AnalysisException {
        ArrayList<Expr> children = new ArrayList<>();
        analyzeCompoundPredicate(cp, children);
        Set<String> names = new HashSet<>();
        // check whether left.columnName equals to right.columnName
        for (Expr child : children) {
            String name = ((SlotRef) child.getChild(0)).getColumnName();
            if (names.contains(name)) {
                throw new AnalysisException("column names on both sides of operator AND should be diffrent");
            }
            names.add(name);
            analyzeSubPredicate(child);
        }
    }

    private void analyzeCompoundPredicate(Expr expr, List<Expr> exprs) throws AnalysisException {
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate cp = (CompoundPredicate) expr;
            if (cp.getOp() != CompoundPredicate.Operator.AND) {
                throw new AnalysisException("Only allow compound predicate with operator AND");
            }
            analyzeCompoundPredicate(expr.getChild(0), exprs);
            analyzeCompoundPredicate(expr.getChild(1), exprs);
        } else {
            exprs.add(expr);
        }
    }

    protected int analyzeColumn(String columnName) throws AnalysisException {
        return LoadProcDir.analyzeCopyColumn(columnName);
    }
}
