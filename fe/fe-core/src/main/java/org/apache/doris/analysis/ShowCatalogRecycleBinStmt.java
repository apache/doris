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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.function.Predicate;

public class ShowCatalogRecycleBinStmt extends ShowStmt {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Type").add("Name").add("DbId").add("TableId").add("PartitionId").add("DropTime")
            .add("DataSize").add("RemoteDataSize").build();

    private Expr where;
    private String nameValue;
    private boolean isAccurateMatch;

    public ShowCatalogRecycleBinStmt(Expr where) {
        this.where = where;
    }

    public String getNameValue() {
        return nameValue;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }

        if (where == null) {
            return;
        }
        boolean valid = analyzeWhereClause();
        if (!valid) {
            throw new AnalysisException("Where clause should like: Name = \"name\", "
                + " or Name LIKE \"matcher\"");
        }
    }

    private boolean analyzeWhereClause() {
        if (!(where instanceof LikePredicate) && !(where instanceof BinaryPredicate)) {
            return false;
        }

        if (where instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) where;
            if (BinaryPredicate.Operator.EQ != binaryPredicate.getOp()) {
                return false;
            }
            isAccurateMatch = true;
        }

        if (where instanceof LikePredicate) {
            LikePredicate likePredicate = (LikePredicate) where;
            if (LikePredicate.Operator.LIKE != likePredicate.getOp()) {
                return false;
            }
        }

        // left child
        if (!(where.getChild(0) instanceof SlotRef)) {
            return false;
        }
        String leftKey = ((SlotRef) where.getChild(0)).getColumnName();
        if (!"name".equalsIgnoreCase(leftKey)) {
            return false;
        }

        // right child
        if (!(where.getChild(1) instanceof StringLiteral)) {
            return false;
        }
        nameValue = ((StringLiteral) where.getChild(1)).getStringValue();
        if (Strings.isNullOrEmpty(nameValue)) {
            return false;
        }

        return true;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public String toSql() {
        StringBuilder builder = new StringBuilder();
        builder.append("SHOW CATALOG RECYCLE BIN");

        builder.append(where.toSql());
        return builder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    public boolean isAccurateMatch() {
        return isAccurateMatch;
    }

    public Expr getWhere() {
        return where;
    }

    public Predicate<String> getNamePredicate() throws AnalysisException {
        if (null == where) {
            return name -> true;
        }
        if (isAccurateMatch) {
            return CaseSensibility.PARTITION.getCaseSensibility()
                    ? name -> name.equals(nameValue) : name -> name.equalsIgnoreCase(nameValue);
        } else {
            PatternMatcher patternMatcher = PatternMatcherWrapper.createMysqlPattern(
                    nameValue, CaseSensibility.PARTITION.getCaseSensibility());
            return patternMatcher::match;
        }
    }
}
