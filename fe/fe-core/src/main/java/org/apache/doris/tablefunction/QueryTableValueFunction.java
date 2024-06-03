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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.jdbc.JdbcExternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public abstract class QueryTableValueFunction extends TableValuedFunctionIf {
    public static final Logger LOG = LogManager.getLogger(QueryTableValueFunction.class);
    public static final String NAME = "query";
    private static final String CATALOG = "catalog";
    private static final String QUERY = "query";
    protected CatalogIf catalogIf;
    protected final String query;

    public QueryTableValueFunction(Map<String, String> params) throws AnalysisException {
        String catalogName = params.get(CATALOG);
        this.query = params.get(QUERY);
        this.catalogIf = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
    }

    public static QueryTableValueFunction createQueryTableValueFunction(Map<String, String> params)
            throws AnalysisException {
        if (params.size() != 2) {
            throw new AnalysisException("Query TableValueFunction must have 2 arguments: 'catalog' and 'query'");
        }
        if (!params.containsKey(CATALOG) || !params.containsKey(QUERY)) {
            throw new AnalysisException("Query TableValueFunction must have 2 arguments: 'catalog' and 'query'");
        }
        String catalogName = params.get(CATALOG);

        // check priv
        UserIdentity userIdentity = ConnectContext.get().getCurrentUserIdentity();
        if (!Env.getCurrentEnv().getAuth().checkCtlPriv(userIdentity, catalogName, PrivPredicate.SELECT)) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "user " + userIdentity + " has no privilege to query in catalog " + catalogName);
        }

        CatalogIf catalogIf = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalogIf == null) {
            throw new AnalysisException("Catalog not found: " + catalogName);
        }
        if (catalogIf instanceof JdbcExternalCatalog) {
            return new JdbcQueryTableValueFunction(params);
        } else {
            throw new AnalysisException(
                    "Catalog not supported query tvf: " + catalogName + ", catalog type:" + catalogIf.getType());
        }
    }

    @Override
    public String getTableName() {
        return "QueryTableValueFunction";
    }

    @Override
    public abstract List<Column> getTableColumns() throws AnalysisException;

    @Override
    public abstract ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc);
}
