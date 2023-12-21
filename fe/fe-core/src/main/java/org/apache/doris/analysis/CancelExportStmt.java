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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.ExportJobState;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;


/**
 * CANCEL EXPORT statement used to cancel export job.
 * syntax:
 *     CANCEL EXPORT [FROM db]
 *     WHERE [LABEL = "export_label" | LABEL like "label_pattern" | STATE = "PENDING/IN_QUEUE/EXPORTING"]
 **/
public class CancelExportStmt extends DdlStmt {

    private static final ImmutableSet<String> SUPPORT_COLUMNS = new ImmutableSet.Builder<String>()
            .add("label")
            .add("state")
            .build();
    @Getter
    private String dbName;

    @Getter
    private CompoundPredicate.Operator operator;

    @Getter
    private String label;

    @Getter
    private String state;

    private Expr whereClause;

    public CancelExportStmt(String dbName, Expr whereClause) {
        this.dbName = dbName;
        this.whereClause = whereClause;
    }

    private void checkColumn(Expr expr, boolean like) throws AnalysisException {
        String inputCol = ((SlotRef) expr.getChild(0)).getColumnName();
        if (!SUPPORT_COLUMNS.contains(inputCol.toLowerCase())) {
            throw new AnalysisException("Current only support label and state, invalid column: " + inputCol);
        }
        if (!(expr.getChild(1) instanceof StringLiteral)) {
            throw new AnalysisException("Value must be a string");
        }

        String inputValue = expr.getChild(1).getStringValue();
        if (Strings.isNullOrEmpty(inputValue)) {
            throw new AnalysisException("Value can't be null");
        }

        if (inputCol.equalsIgnoreCase("label")) {
            label = inputValue;
        }

        if (inputCol.equalsIgnoreCase("state")) {
            if (like) {
                throw new AnalysisException("Only label can use like");
            }
            state = inputValue;
            ExportJobState jobState = ExportJobState.valueOf(state);
            if (jobState != ExportJobState.PENDING
                    && jobState != ExportJobState.EXPORTING) {
                throw new AnalysisException("Only support PENDING/EXPORTING, invalid state: " + state);
            }
        }
    }

    private void likeCheck(Expr expr) throws AnalysisException {
        LikePredicate likePredicate = (LikePredicate) expr;
        boolean like = LikePredicate.Operator.LIKE.equals(likePredicate.getOp());
        if (!like) {
            throw new AnalysisException("Not support REGEXP");
        }
        checkColumn(expr, true);
    }

    private void binaryCheck(Expr expr) throws AnalysisException {
        BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
        if (!Operator.EQ.equals(binaryPredicate.getOp())) {
            throw new AnalysisException("Only support equal or like");
        }
        checkColumn(expr, false);
    }

    private void compoundCheck(Expr expr) throws AnalysisException {
        // current only support label and state
        CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
        if (CompoundPredicate.Operator.NOT == compoundPredicate.getOp()) {
            throw new AnalysisException("Current not support NOT operator");
        }
        for (int i = 0; i < 2; i++) {
            Expr child = compoundPredicate.getChild(i);
            if (child instanceof CompoundPredicate) {
                throw new AnalysisException("Current not support nested clause");
            } else if (child instanceof LikePredicate) {
                likeCheck(child);
            } else if (child instanceof BinaryPredicate) {
                binaryCheck(child);
            } else {
                throw new AnalysisException("Only support like/binary predicate");
            }
        }
        operator = compoundPredicate.getOp();
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                throw new AnalysisException("No database selected");
            }
        }

        if (null == whereClause) {
            throw new AnalysisException("Where clause can't be null");
        } else if (whereClause instanceof LikePredicate) {
            likeCheck(whereClause);
        } else if (whereClause instanceof BinaryPredicate) {
            binaryCheck(whereClause);
        } else if (whereClause instanceof CompoundPredicate) {
            compoundCheck(whereClause);
        } else {
            throw new AnalysisException("Only support like/binary/compound predicate");
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CANCEL EXPORT ");
        if (!Strings.isNullOrEmpty(dbName)) {
            stringBuilder.append("FROM ").append(dbName);
        }

        if (whereClause != null) {
            stringBuilder.append(" WHERE ").append(whereClause.toSql());
        }
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

}
