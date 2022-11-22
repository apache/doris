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
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.loadv2.JobState;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import lombok.Getter;

import java.util.Set;


/**
 * CANCEL LOAD statement used to cancel load job.
 * syntax:
 *     CANCEL LOAD [FROM db] WHERE load_label (= "xxx" | LIKE "xxx")
 **/
public class CancelLoadStmt extends DdlStmt {

    private static final Set<String> SUPPORT_COLUMNS = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

    @Getter
    private String dbName;

    @Getter
    private CompoundPredicate.Operator operator;

    @Getter
    private String label;

    @Getter
    private String state;

    private Expr whereClause;

    public CancelLoadStmt(String dbName, Expr whereClause) {
        this.dbName = dbName;
        this.whereClause = whereClause;
        this.SUPPORT_COLUMNS.add("label");
        this.SUPPORT_COLUMNS.add("state");
    }

    private void checkColumn(Expr expr, boolean like) throws AnalysisException {
        String inputCol = ((SlotRef) expr.getChild(0)).getColumnName();
        if (!SUPPORT_COLUMNS.contains(inputCol)) {
            throw new AnalysisException("Current not support " + inputCol);
        }
        if (!(expr.getChild(1) instanceof StringLiteral)) {
            throw new AnalysisException("Value must is string");
        }

        String inputValue = expr.getChild(1).getStringValue();
        if (Strings.isNullOrEmpty(inputValue)) {
            throw new AnalysisException("Value can't is null");
        }
        if (like && !inputValue.contains("%")) {
            inputValue = "%" + inputValue + "%";
        }
        if (inputCol.equalsIgnoreCase("label")) {
            label = inputValue;
        }
        if (inputCol.equalsIgnoreCase("state")) {
            if (like) {
                throw new AnalysisException("Only label can use like");
            }
            state = inputValue;
            try {
                JobState jobState = JobState.valueOf(state);
                if (jobState != JobState.PENDING && jobState != JobState.ETL && jobState != JobState.LOADING) {
                    throw new AnalysisException("invalid state: " + state);
                }
            } catch (IllegalArgumentException e) {
                throw new AnalysisException("invalid state: " + state);
            }
        }
    }

    private void likeCheck(Expr expr) throws AnalysisException {
        if (expr instanceof LikePredicate) {
            LikePredicate likePredicate = (LikePredicate) expr;
            boolean like = LikePredicate.Operator.LIKE.equals(likePredicate.getOp());
            if (!like) {
                throw new AnalysisException("Not support REGEXP");
            }
            checkColumn(expr, true);
        }
    }

    private void binaryCheck(Expr expr) throws AnalysisException {
        if (expr instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
            if (!Operator.EQ.equals(binaryPredicate.getOp())) {
                throw new AnalysisException("Only support equal or like");
            }
            checkColumn(expr, false);
        }
    }

    private void compoundCheck(Expr expr) throws AnalysisException {
        if (expr == null) {
            throw new AnalysisException("Where clause can't is null");
        }
        if (expr instanceof CompoundPredicate) {
            // current only support label and state
            CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
            for (int i = 0; i < 2; i++) {
                Expr child = compoundPredicate.getChild(i);
                if (child instanceof CompoundPredicate) {
                    throw new AnalysisException("Current only support label and state");
                }
                likeCheck(child);
                binaryCheck(child);
            }
            operator = compoundPredicate.getOp();
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                throw new AnalysisException("No database selected");
            }
        } else {
            dbName = ClusterNamespace.getFullName(getClusterName(), dbName);
        }

        // check auth after we get real load job
        // analyze expr
        likeCheck(whereClause);
        binaryCheck(whereClause);
        compoundCheck(whereClause);
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CANCEL LOAD ");
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
