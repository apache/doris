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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

/**
 * show typecast command
 */
public class ShowTypeCastCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("Origin Type", ScalarType.createVarchar(128)))
                .addColumn(new Column("Cast Type", ScalarType.createVarchar(128)))
                .build();
    private String dbName;

    /**
     * struct of ShowTypeCastCommand
     */
    public ShowTypeCastCommand(String dbName) {
        super(PlanType.SHOW_TYPE_CAST_COMMAND);
        this.dbName = dbName;
    }

    private void analyze(ConnectContext ctx) throws UserException {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
    }

    @VisibleForTesting
    protected ShowResultSet handleShowTypeCast(ConnectContext ctx, StmtExecutor executor) throws Exception {
        analyze(ctx);

        Util.prohibitExternalCatalog(ctx.getDefaultCatalog(), this.getClass().getSimpleName());
        DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(dbName);

        List<List<String>> resultRowSet = Lists.newArrayList();
        ImmutableSetMultimap<PrimitiveType, PrimitiveType> castMap = PrimitiveType.getImplicitCastMap();
        if (db instanceof Database) {
            resultRowSet = castMap.entries().stream().map(primitiveTypePrimitiveTypeEntry -> {
                List<String> list = Lists.newArrayList();
                list.add(primitiveTypePrimitiveTypeEntry.getKey().toString());
                list.add(primitiveTypePrimitiveTypeEntry.getValue().toString());
                return list;
            }).collect(Collectors.toList());
        }

        // Only success
        return new ShowResultSet(getMetaData(), resultRowSet);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowTypeCast(ctx, executor);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTypeCastCommand(this, context);
    }
}
