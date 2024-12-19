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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Objects;

/**
 * Command for DROP CATALOG.
 */
public class DropCatalogCommand extends DropCommand {
    private final String catalogName;
    private final boolean ifExists;

    public DropCatalogCommand(String catalogName, boolean ifExists) {
        super(PlanType.DROP_CATALOG_COMMAND);
        this.catalogName = Objects.requireNonNull(catalogName, "Catalog name cannot be null");
        this.ifExists = ifExists;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // Validate the catalog name
        Util.checkCatalogAllRules(catalogName);

        if (catalogName.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            throw new AnalysisException("Internal catalog can't be drop.");
        }

        if (!Env.getCurrentEnv().getAccessManager().checkCtlPriv(
                ConnectContext.get(), catalogName, PrivPredicate.DROP)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                    ConnectContext.get().getQualifiedUser(), catalogName);
        }

        Env.getCurrentEnv().getCatalogMgr().dropCatalog(catalogName, ifExists);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropCatalogCommand(this, context);
    }

    public String getCatalogName() {
        return catalogName;
    }

    public boolean isIfExists() {
        return ifExists;
    }
}
