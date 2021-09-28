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

// CANCEL LOAD statement used to cancel load job.
//
// syntax:
//      CANCEL LOAD [FROM db] WHERE load_label (= "xxx" | LIKE "xxx")
public class CancelLoadStmt extends DdlStmt {

    private String dbName;
    private String label;

    private Expr whereClause;
    private boolean isAccurateMatch;

    public String getDbName() {
        return dbName;
    }

    public String getLabel() {
        return label;
    }

    public CancelLoadStmt(String dbName, Expr whereClause) {
        this.dbName = dbName;
        this.whereClause = whereClause;
        this.isAccurateMatch = false;
    }

    public boolean isAccurateMatch() {
        return isAccurateMatch;
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

        // analyze expr if not null
        boolean valid = true;
        do {
            if (whereClause == null) {
                valid = false;
                break;
            }

            if (whereClause instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) whereClause;
                isAccurateMatch = true;
                if (binaryPredicate.getOp() != Operator.EQ) {
                    valid = false;
                    break;
                }
            } else if (whereClause instanceof LikePredicate) {
                LikePredicate likePredicate = (LikePredicate) whereClause;
                if (likePredicate.getOp() != LikePredicate.Operator.LIKE) {
                    valid = false;
                    break;
                }
            } else {
                valid = false;
                break;
            }

            // left child
            if (!(whereClause.getChild(0) instanceof SlotRef)) {
                valid = false;
                break;
            }
            if (!((SlotRef) whereClause.getChild(0)).getColumnName().equalsIgnoreCase("label")) {
                valid = false;
                break;
            }

            // right child
            if (!(whereClause.getChild(1) instanceof StringLiteral)) {
                valid = false;
                break;
            }

            label = ((StringLiteral) whereClause.getChild(1)).getStringValue();
            if (Strings.isNullOrEmpty(label)) {
                valid = false;
                break;
            }
            if (!isAccurateMatch && !label.contains("%")) {
                label = "%" + label + "%";
            }
        } while (false);

        if (!valid) {
            throw new AnalysisException("Where clause should looks like: LABEL = \"your_load_label\"," +
                    " or LABEL LIKE \"matcher\"");
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CANCEL LOAD ");
        if (!Strings.isNullOrEmpty(dbName)) {
            stringBuilder.append("FROM " + dbName);
        }

        if (whereClause != null) {
            stringBuilder.append(" WHERE " + whereClause.toSql());
        }
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

}
