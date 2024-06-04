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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * show procedure status command
 */
public class ShowProcedureStatusCommand extends Command implements NoForward {
    public static final Logger LOG = LogManager.getLogger(ShowProcedureStatusCommand.class);
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>().add("ProcedureName")
            .add("CatalogId").add("DbId").add("DbName").add("PackageName").add("OwnerName").add("CreateTime")
            .add("ModifyTime").build();
    private static final String colDb = "db";
    private static final String colProcName = "procedurename";
    private static final String colName = "name";
    private final Set<Expression> whereExpr;

    /**
     * constructor
     */
    public ShowProcedureStatusCommand(final Set<Expression> whereExpr) {
        super(PlanType.SHOW_PROCEDURE_COMMAND);
        this.whereExpr = Objects.requireNonNull(whereExpr, "whereExpr should not be null");
    }

    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createStringType()));
        }
        return builder.build();
    }

    private void validateAndExtractFilters(StringBuilder dbFilter, StringBuilder procFilter) throws Exception {

        if (whereExpr.isEmpty()) {
            return;
        }
        Set<Expression> likeSet = whereExpr.stream().filter(Like.class::isInstance).collect(Collectors.toSet());
        Set<Expression> equalTo = whereExpr.stream().filter(EqualTo.class::isInstance).collect(Collectors.toSet());

        if (whereExpr.size() != likeSet.size() + equalTo.size()) {
            throw new AnalysisException("only support AND conjunction, does not support OR.");
        }

        equalTo.addAll(likeSet);
        Map<String, String> filterMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        filterMap = equalTo.stream()
                .collect(ImmutableMap.toImmutableMap(exp -> ((Slot) exp.child(0)).getName().toLowerCase(),
                        exp -> ((Literal) exp.child(1)).getStringValue().toLowerCase(),
                        (a, b) -> {
                            throw new AnalysisException("WhereClause can contain one predicate for one column.");
                        }));

        // we support filter on Db and Name and ProcedureName.
        // But one column we can put only once and support conjuncts
        for (Map.Entry<String, String> elem : filterMap.entrySet()) {
            String columnName = elem.getKey();
            if ((!columnName.toLowerCase().equals(colDb))
                    && (!columnName.toLowerCase().equals(colName))
                    && (!columnName.toLowerCase().equals(colProcName))) {
                throw new AnalysisException("Only supports filter " + colProcName + ", "
                        + colName + "," + colProcName + " with equalTo or LIKE");
            }
            if (columnName.toLowerCase().equals(colDb)) {
                if (dbFilter.length() != 0) {
                    throw new AnalysisException("Only supports filter Db only 1 time in where clause");
                }
                dbFilter.append(elem.getValue().toLowerCase());
            } else if ((columnName.toLowerCase().equals(colName)) || (columnName.toLowerCase().equals(colProcName))) {
                if (procFilter.length() != 0) {
                    throw new AnalysisException("Only supports filter Name/ProcedureName only 1 time in where clause");
                }
                procFilter.append(elem.getValue().toLowerCase());
            }
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        StringBuilder dbFilter = new StringBuilder();
        StringBuilder procFilter = new StringBuilder();
        validateAndExtractFilters(dbFilter, procFilter);

        List<List<String>> results = new ArrayList<>();
        ctx.getPlSqlOperation().getExec().functions.showProcedure(results, dbFilter.toString(), procFilter.toString());
        ShowResultSet commonResultSet = new ShowResultSet(getMetaData(), results);
        executor.sendResultSet(commonResultSet);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowProcedureStatusCommand(this, context);
    }
}
