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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.qe.ConnectContext;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Shared context for one FE-side incremental refresh attempt.
 *
 * <p>TSO positions are now obtained from {@code OlapTableStream} per-partition offsets
 * in {@link IvmDeltaRewriteState} rather than stored here.
 */
public class IvmIncrRefreshContext {
    private final MTMV mtmv;
    private final ConnectContext connectContext;
    private final IvmRewriteResult rewriteResult;
    private final boolean includeExhaustedStreams;
    private final String auditStmt;
    private final Consumer<String> queryIdConsumer;

    public IvmIncrRefreshContext(MTMV mtmv, ConnectContext connectContext, IvmRewriteResult rewriteResult,
            boolean includeExhaustedStreams) {
        this(mtmv, connectContext, rewriteResult, includeExhaustedStreams, null, null);
    }

    public IvmIncrRefreshContext(MTMV mtmv, ConnectContext connectContext,
            String auditStmt, Consumer<String> queryIdConsumer) {
        this(mtmv, connectContext, null, false,
                Objects.requireNonNull(auditStmt, "auditStmt can not be null"),
                Objects.requireNonNull(queryIdConsumer, "queryIdConsumer can not be null"));
    }

    private IvmIncrRefreshContext(MTMV mtmv, ConnectContext connectContext, IvmRewriteResult rewriteResult,
            boolean includeExhaustedStreams, String auditStmt, Consumer<String> queryIdConsumer) {
        this.mtmv = Objects.requireNonNull(mtmv, "mtmv can not be null");
        this.connectContext = Objects.requireNonNull(connectContext, "connectContext can not be null");
        this.rewriteResult = rewriteResult;
        this.includeExhaustedStreams = includeExhaustedStreams;
        this.auditStmt = auditStmt;
        this.queryIdConsumer = queryIdConsumer;
    }

    public MTMV getMtmv() {
        return mtmv;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    /** Returns the IVM rewrite result captured during MV query rewriting. */
    public IvmRewriteResult getRewriteResult() {
        return rewriteResult;
    }

    public boolean isIncludeExhaustedStreams() {
        return includeExhaustedStreams;
    }

    public String getAuditStmt() {
        return auditStmt;
    }

    public Consumer<String> getQueryIdConsumer() {
        return queryIdConsumer;
    }

    static TableNameInfo toTableNameInfo(LogicalOlapScan scan) {
        OlapTable table = scan.getTable();
        if (table == null) {
            return null;
        }
        TableNameInfo tableNameInfo = TableNameInfoUtils.fromTableOrNull(table);
        if (tableNameInfo != null) {
            return tableNameInfo;
        }
        String dbName = table.getDBName();
        String ctlName = InternalCatalog.INTERNAL_CATALOG_NAME;
        List<String> qualifier = scan.getQualifier();
        if (qualifier.size() >= 1) {
            dbName = qualifier.get(qualifier.size() - 1);
        }
        if (qualifier.size() >= 2) {
            ctlName = qualifier.get(qualifier.size() - 2);
        }
        return new TableNameInfo(ctlName, dbName, table.getName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IvmIncrRefreshContext that = (IvmIncrRefreshContext) o;
        return Objects.equals(mtmv, that.mtmv)
                && Objects.equals(connectContext, that.connectContext)
                && Objects.equals(rewriteResult, that.rewriteResult)
                && includeExhaustedStreams == that.includeExhaustedStreams;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mtmv, connectContext, rewriteResult, includeExhaustedStreams);
    }

    @Override
    public String toString() {
        return "IvmIncrRefreshContext{"
                + "mtmv=" + mtmv.getName()
                + '}';
    }
}
