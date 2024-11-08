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

package org.apache.doris.nereids.trees.plans.commands.refresh;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogLog;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.persist.OperationType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Manually refresh the catalog metadata.
 */
public class RefreshCatalogCommand extends Command implements ForwardWithSync {
    private static final Logger LOG = LogManager.getLogger(RefreshCatalogCommand.class);
    private static final String INVALID_CACHE = "invalid_cache";
    private final String catalogName;
    private Map<String, String> properties;
    private boolean invalidCache = true;

    public RefreshCatalogCommand(String catalogName, Map<String, String> properties) {
        super(PlanType.REFRESH_CATALOG_COMMAND);
        this.catalogName = catalogName;
        this.properties = properties;
    }

    private void validate() throws AnalysisException {
        Util.checkCatalogAllRules(catalogName);
        if (catalogName.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            throw new AnalysisException("Internal catalog name can't be refresh.");
        }

        if (!Env.getCurrentEnv().getAccessManager().checkCtlPriv(
                ConnectContext.get(), catalogName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CATALOG_ACCESS_DENIED_ERROR,
                    PrivPredicate.SHOW.getPrivs().toString(), catalogName);
        }

        if (properties != null) {
            // Set to false only if user set the property "invalid_cache"="false"
            invalidCache = !(properties.get(INVALID_CACHE) != null && properties.get(INVALID_CACHE)
                    .equalsIgnoreCase("false"));
        }

    }

    private void refreshCatalogInternal(CatalogIf catalog) {
        if (!catalogName.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            ((ExternalCatalog) catalog).onRefreshCache(invalidCache);
            LOG.info("refresh catalog {} with invalidCache {}", catalogName, invalidCache);
        }
    }

    /**
     * refresh catalog
     */
    public void handleRefreshCatalog() throws AnalysisException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalogName);
        CatalogLog log = new CatalogLog();
        log.setCatalogId(catalog.getId());
        log.setInvalidCache(invalidCache);
        refreshCatalogInternal(catalog);
        Env.getCurrentEnv().getEditLog().logCatalogLog(OperationType.OP_REFRESH_CATALOG, log);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        handleRefreshCatalog();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitRefreshCatalogCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.REFRESH;
    }

    /**
     * return sql expression of this command
     *
     * @return sql command
     */
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("REFRESH CATALOG ")
                .append("`")
                .append(catalogName)
                .append("`");
        return stringBuilder.toString();
    }
}
