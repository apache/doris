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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;

import java.util.List;
import java.util.Objects;

/**
 * Represents the command for SHOW CREATE CATALOG.
 */
public class ShowCreateCatalogCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Catalog", ScalarType.createVarchar(20)))
                    .addColumn(new Column("CreateCatalog", ScalarType.createVarchar(30)))
                    .build();

    private final String catalogName;

    public ShowCreateCatalogCommand(String catalogName) {
        super(PlanType.SHOW_CREATE_CATALOG_COMMAND);
        this.catalogName = Objects.requireNonNull(catalogName, "Catalog name cannot be null");
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (Strings.isNullOrEmpty(catalogName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_CATALOG_NAME, catalogName);
        }

        if (!Env.getCurrentEnv().getAccessManager()
                .checkCtlPriv(ConnectContext.get(), catalogName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                    ConnectContext.get().getQualifiedUser(), catalogName);
        }

        List<List<String>> rows = Env.getCurrentEnv().getCatalogMgr().showCreateCatalog(catalogName);

        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateCatalogCommand(this, context);
    }

    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
