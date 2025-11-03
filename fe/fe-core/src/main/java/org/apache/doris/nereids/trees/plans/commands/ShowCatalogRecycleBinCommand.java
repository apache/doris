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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * ShowCatalogRecycleBinCommand
 */
public class ShowCatalogRecycleBinCommand extends ShowCommand {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Type")
            .add("Name")
            .add("DbId")
            .add("TableId")
            .add("PartitionId")
            .add("DropTime")
            .add("DataSize")
            .add("RemoteDataSize")
            .build();

    private Expression whereClause;
    private String nameValue;
    private boolean isAccurateMatch;

    /**
     * constructor
     */
    public ShowCatalogRecycleBinCommand(Expression whereClause) {
        super(PlanType.SHOW_CATALOG_RECYCLE_BIN_COMMAND);
        this.whereClause = whereClause;
    }

    /**
     * get meta for show tabletId
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    /**
     * validate
     */
    public void validate() throws AnalysisException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }

        if (!analyzeWhereClause()) {
            throw new AnalysisException("Where clause should like: Name = \"name\", "
                    + " or Name LIKE \"matcher\"");
        }
    }

    private boolean analyzeWhereClause() {
        if (whereClause == null) {
            return true;
        } else if (whereClause instanceof EqualTo) {
            EqualTo equalTo = (EqualTo) whereClause;
            if (equalTo.left() instanceof UnboundSlot
                    && equalTo.child(0).toSql().toLowerCase(Locale.ROOT).equals("name")
                    && equalTo.right() instanceof Literal) {
                nameValue = ((Literal) equalTo.right()).getStringValue();
            } else {
                return false;
            }
            isAccurateMatch = true;
        } else if (whereClause instanceof Like) {
            Like like = (Like) whereClause;
            if (like.left() instanceof UnboundSlot
                    && like.child(0).toSql().toLowerCase(Locale.ROOT).equals("name")
                    && like.right() instanceof Literal) {
                nameValue = ((Literal) like.right()).getStringValue();
            } else {
                return false;
            }
        } else {
            return false;
        }
        return !Strings.isNullOrEmpty(nameValue);
    }

    private Predicate<String> getNamePredicate() throws AnalysisException {
        if (whereClause == null) {
            return name -> true;
        }
        if (isAccurateMatch) {
            return CaseSensibility.PARTITION.getCaseSensibility()
                    ? name -> name.equals(nameValue)
                    : name -> name.equalsIgnoreCase(nameValue);
        } else {
            PatternMatcher patternMatcher = PatternMatcherWrapper.createMysqlPattern(
                    nameValue, CaseSensibility.PARTITION.getCaseSensibility());
            return patternMatcher::match;
        }
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        Predicate<String> predicate = getNamePredicate();
        List<List<String>> infos = Env.getCurrentRecycleBin().getInfo().stream()
                .filter(i -> predicate.test(i.get(1)))
                .collect(Collectors.toList());
        return new ShowResultSet(getMetaData(), infos);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCatalogRecycleBinCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
