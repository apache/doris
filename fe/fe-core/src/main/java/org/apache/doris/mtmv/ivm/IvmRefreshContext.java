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
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Shared immutable context for one FE-side incremental refresh attempt.
 */
public class IvmRefreshContext {
    private final MTMV mtmv;
    private final ConnectContext connectContext;
    private final IvmNormalizeResult normalizeResult;
    private final Map<TableNameInfo, IvmStreamRef> baseTableStreams;

    public IvmRefreshContext(MTMV mtmv, ConnectContext connectContext) {
        this(mtmv, connectContext, null, null);
    }

    public IvmRefreshContext(MTMV mtmv, ConnectContext connectContext, IvmNormalizeResult normalizeResult) {
        this(mtmv, connectContext, normalizeResult, null);
    }

    public IvmRefreshContext(MTMV mtmv, ConnectContext connectContext,
            IvmNormalizeResult normalizeResult, Map<TableNameInfo, IvmStreamRef> baseTableStreams) {
        this.mtmv = Objects.requireNonNull(mtmv, "mtmv can not be null");
        this.connectContext = Objects.requireNonNull(connectContext, "connectContext can not be null");
        this.normalizeResult = normalizeResult;
        this.baseTableStreams = baseTableStreams == null ? ImmutableMap.of() : ImmutableMap.copyOf(baseTableStreams);
    }

    public MTMV getMtmv() {
        return mtmv;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    /** Returns the IVM normalize result captured during MV query normalization. */
    public IvmNormalizeResult getNormalizeResult() {
        return normalizeResult;
    }

    public Map<TableNameInfo, IvmStreamRef> getBaseTableStreams() {
        return baseTableStreams;
    }

    public IvmStreamRef getBaseTableStream(LogicalOlapScan scan) {
        TableNameInfo tableNameInfo = toTableNameInfo(scan);
        return tableNameInfo == null ? null : baseTableStreams.get(tableNameInfo);
    }

    static Map<TableNameInfo, IvmStreamRef> buildBaseTableStreams(
            Map<BaseTableInfo, IvmStreamRef> baseTableStreams) {
        if (baseTableStreams == null || baseTableStreams.isEmpty()) {
            return ImmutableMap.of();
        }
        Map<TableNameInfo, IvmStreamRef> streamsByName = new HashMap<>();
        for (Map.Entry<BaseTableInfo, IvmStreamRef> entry : baseTableStreams.entrySet()) {
            BaseTableInfo baseTableInfo = entry.getKey();
            if (baseTableInfo.isValid()) {
                streamsByName.put(new TableNameInfo(baseTableInfo.getCtlName(),
                        baseTableInfo.getDbName(), baseTableInfo.getTableName()), entry.getValue());
            }
        }
        return ImmutableMap.copyOf(streamsByName);
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
        IvmRefreshContext that = (IvmRefreshContext) o;
        return Objects.equals(mtmv, that.mtmv)
                && Objects.equals(connectContext, that.connectContext)
                && Objects.equals(normalizeResult, that.normalizeResult)
                && Objects.equals(baseTableStreams, that.baseTableStreams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mtmv, connectContext, normalizeResult, baseTableStreams);
    }

    @Override
    public String toString() {
        return "IvmRefreshContext{"
                + "mtmv=" + mtmv.getName()
                + '}';
    }
}
