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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.List;


// CANCEL LOAD statement used to cancel load job.
//
// syntax:
//      CANCEL LOAD [FROM db] WHERE load_label (= "xxx" | LIKE "xxx")
public class CancelLoadStmt extends DdlStmt {

    @Getter
    private String dbName;

    @Getter
    private CompoundPredicate.Operator operator;

    @Getter
    private String label;

    @Getter
    private String state;

    private Expr whereClause;

    private static final List<String> SUPPORT_COLUMNS = Lists.newArrayList("label", "state");

    public CancelLoadStmt(String dbName, Expr whereClause) {
        this.dbName = dbName;
        this.whereClause = whereClause;
    }

    private void checkColumn(Expr expr, boolean like) throws AnalysisException {
        String inputCol = ((SlotRef) expr.getChild(0)).getColumnName().toLowerCase();
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
        if (inputCol.equals("label")) {
            label = inputValue;
        }
        if (inputCol.equals("state")) {
            state = inputValue;
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
            BinaryPredicate binaryPredicate = (BinaryPredicate) whereClause;
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
            CompoundPredicate compoundPredicate = (CompoundPredicate) whereClause;
            for (int i = 0; i < 2; i++) {
                Expr child = compoundPredicate.getChild(i);
                if (child instanceof CompoundPredicate) {
                    throw new AnalysisException("Current only support label and state");
                }
                likeCheck(expr);
                binaryCheck(expr);
            }
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
