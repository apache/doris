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

package org.apache.doris.nereids;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.mysql.privilege.RowFilterPolicy;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Variable;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.proto.Types.PUniqueId;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.cache.CacheProxy;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** SqlCacheContext */
public class SqlCacheContext {
    private final UserIdentity userIdentity;
    private final TUniqueId queryId;
    // if contains udf/udaf/tableValuesFunction we can not process it and skip use sql cache
    private volatile boolean cannotProcessExpression;
    private volatile String originSql;
    private volatile String physicalPlan;
    private volatile long latestPartitionId = -1;
    private volatile long latestPartitionTime = -1;
    private volatile long latestPartitionVersion = -1;
    private volatile long sumOfPartitionNum = -1;
    private final Set<FullTableName> usedTables = Sets.newLinkedHashSet();
    // value: ddl sql
    private final Map<FullTableName, String> usedViews = Maps.newLinkedHashMap();
    // value: usedColumns
    private final Map<FullTableName, Set<String>> checkPrivilegeTablesOrViews = Maps.newLinkedHashMap();
    private final Map<FullTableName, List<RowFilterPolicy>> rowPolicies = Maps.newLinkedHashMap();
    private final Map<FullColumnName, Optional<DataMaskPolicy>> dataMaskPolicies = Maps.newLinkedHashMap();
    private final Set<Variable> usedVariables = Sets.newLinkedHashSet();
    // key: the expression which **contains** nondeterministic function, e.g. date_add(date_column, date(now()))
    // value: the expression which already try to fold nondeterministic function,
    // e.g. date_add(date_column, '2024-01-01')
    // note that value maybe contains nondeterministic function too, when fold failed
    private final List<Pair<Expression, Expression>> foldFullNondeterministicPairs = Lists.newArrayList();
    // key: the expression which **is** nondeterministic function, e.g. now()
    // value: the expression which already try to fold nondeterministic function, e.g. '2024-01-01 10:01:03'
    private final List<Pair<Expression, Expression>> foldNondeterministicPairs = Lists.newArrayList();
    private volatile boolean hasUnsupportedTables;
    private final List<ScanTable> scanTables = Lists.newArrayList();
    private volatile CacheProxy cacheProxy;

    private volatile List<Expr> resultExprs;
    private volatile List<String> colLabels;

    private volatile PUniqueId cacheKeyMd5;
    private volatile ResultSet resultSetInFe;

    private volatile CacheKeyType cacheKeyType = CacheKeyType.SQL;

    public SqlCacheContext(UserIdentity userIdentity, TUniqueId queryId) {
        this.userIdentity = Objects.requireNonNull(userIdentity, "userIdentity cannot be null");
        this.queryId = Objects.requireNonNull(queryId, "queryId cannot be null");
    }

    public String getPhysicalPlan() {
        return physicalPlan;
    }

    public void setPhysicalPlan(String physicalPlan) {
        this.physicalPlan = physicalPlan;
    }

    public void setCannotProcessExpression(boolean cannotProcessExpression) {
        this.cannotProcessExpression = cannotProcessExpression;
    }

    public boolean containsCannotProcessExpression() {
        return cannotProcessExpression;
    }

    public boolean hasUnsupportedTables() {
        return hasUnsupportedTables;
    }

    public void setHasUnsupportedTables(boolean hasUnsupportedTables) {
        this.hasUnsupportedTables = hasUnsupportedTables;
    }

    /** addUsedTable */
    public synchronized void addUsedTable(TableIf tableIf) {
        if (tableIf == null) {
            return;
        }
        DatabaseIf database = tableIf.getDatabase();
        if (database == null) {
            setCannotProcessExpression(true);
            return;
        }
        CatalogIf catalog = database.getCatalog();
        if (catalog == null) {
            setCannotProcessExpression(true);
            return;
        }

        usedTables.add(
                new FullTableName(database.getCatalog().getName(), database.getFullName(), tableIf.getName())
        );
    }

    /** addUsedView */
    public synchronized void addUsedView(TableIf tableIf, String ddlSql) {
        if (tableIf == null) {
            return;
        }
        DatabaseIf database = tableIf.getDatabase();
        if (database == null) {
            setCannotProcessExpression(true);
            return;
        }
        CatalogIf catalog = database.getCatalog();
        if (catalog == null) {
            setCannotProcessExpression(true);
            return;
        }

        usedViews.put(
                new FullTableName(database.getCatalog().getName(), database.getFullName(), tableIf.getName()),
                ddlSql
        );
    }

    /** addNeedCheckPrivilegeTablesOrViews */
    public synchronized void addCheckPrivilegeTablesOrViews(TableIf tableIf, Set<String> usedColumns) {
        if (tableIf == null) {
            return;
        }
        DatabaseIf database = tableIf.getDatabase();
        if (database == null) {
            setCannotProcessExpression(true);
            return;
        }
        CatalogIf catalog = database.getCatalog();
        if (catalog == null) {
            setCannotProcessExpression(true);
            return;
        }
        FullTableName fullTableName = new FullTableName(catalog.getName(), database.getFullName(), tableIf.getName());
        Set<String> existsColumns = checkPrivilegeTablesOrViews.get(fullTableName);
        if (existsColumns == null) {
            checkPrivilegeTablesOrViews.put(fullTableName, usedColumns);
        } else {
            ImmutableSet.Builder<String> allUsedColumns = ImmutableSet.builderWithExpectedSize(
                    existsColumns.size() + usedColumns.size());
            allUsedColumns.addAll(existsColumns);
            allUsedColumns.addAll(usedColumns);
            checkPrivilegeTablesOrViews.put(fullTableName, allUsedColumns.build());
        }
    }

    public synchronized void setRowFilterPolicy(
            String catalog, String db, String table, List<? extends RowFilterPolicy> rowFilterPolicy) {
        rowPolicies.put(new FullTableName(catalog, db, table), Utils.fastToImmutableList(rowFilterPolicy));
    }

    public synchronized Map<FullTableName, List<RowFilterPolicy>> getRowFilterPolicies() {
        return ImmutableMap.copyOf(rowPolicies);
    }

    public synchronized void addDataMaskPolicy(
            String catalog, String db, String table, String columnName, Optional<DataMaskPolicy> dataMaskPolicy) {
        dataMaskPolicies.put(
                new FullColumnName(catalog, db, table, columnName.toLowerCase(Locale.ROOT)), dataMaskPolicy
        );
    }

    public synchronized Map<FullColumnName, Optional<DataMaskPolicy>> getDataMaskPolicies() {
        return ImmutableMap.copyOf(dataMaskPolicies);
    }

    public synchronized void addUsedVariable(Variable value) {
        usedVariables.add(value);
    }

    public synchronized List<Variable> getUsedVariables() {
        return ImmutableList.copyOf(usedVariables);
    }

    public synchronized void addFoldFullNondeterministicPair(Expression unfold, Expression fold) {
        foldFullNondeterministicPairs.add(Pair.of(unfold, fold));
    }

    public synchronized List<Pair<Expression, Expression>> getFoldFullNondeterministicPairs() {
        return ImmutableList.copyOf(foldFullNondeterministicPairs);
    }

    public synchronized void addFoldNondeterministicPair(Expression unfold, Expression fold) {
        foldNondeterministicPairs.add(Pair.of(unfold, fold));
    }

    public synchronized List<Pair<Expression, Expression>> getFoldNondeterministicPairs() {
        return ImmutableList.copyOf(foldNondeterministicPairs);
    }

    public boolean isCannotProcessExpression() {
        return cannotProcessExpression;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public long getLatestPartitionTime() {
        return latestPartitionTime;
    }

    public void setLatestPartitionTime(long latestPartitionTime) {
        this.latestPartitionTime = latestPartitionTime;
    }

    public long getLatestPartitionVersion() {
        return latestPartitionVersion;
    }

    public void setLatestPartitionVersion(long latestPartitionVersion) {
        this.latestPartitionVersion = latestPartitionVersion;
    }

    public long getLatestPartitionId() {
        return latestPartitionId;
    }

    public void setLatestPartitionId(long latestPartitionId) {
        this.latestPartitionId = latestPartitionId;
    }

    public long getSumOfPartitionNum() {
        return sumOfPartitionNum;
    }

    public void setSumOfPartitionNum(long sumOfPartitionNum) {
        this.sumOfPartitionNum = sumOfPartitionNum;
    }

    public CacheProxy getCacheProxy() {
        return cacheProxy;
    }

    public void setCacheProxy(CacheProxy cacheProxy) {
        this.cacheProxy = cacheProxy;
    }

    public Set<FullTableName> getUsedTables() {
        return ImmutableSet.copyOf(usedTables);
    }

    public Map<FullTableName, String> getUsedViews() {
        return ImmutableMap.copyOf(usedViews);
    }

    public synchronized Map<FullTableName, Set<String>> getCheckPrivilegeTablesOrViews() {
        return ImmutableMap.copyOf(checkPrivilegeTablesOrViews);
    }

    public synchronized Map<FullTableName, List<RowFilterPolicy>> getRowPolicies() {
        return ImmutableMap.copyOf(rowPolicies);
    }

    public synchronized void addScanTable(ScanTable scanTable) {
        this.scanTables.add(scanTable);
    }

    public synchronized List<ScanTable> getScanTables() {
        return ImmutableList.copyOf(scanTables);
    }

    public List<Expr> getResultExprs() {
        return resultExprs;
    }

    public void setResultExprs(List<Expr> resultExprs) {
        this.resultExprs = ImmutableList.copyOf(resultExprs);
    }

    public List<String> getColLabels() {
        return colLabels;
    }

    public void setColLabels(List<String> colLabels) {
        this.colLabels = ImmutableList.copyOf(colLabels);
    }

    public TUniqueId getQueryId() {
        return queryId;
    }

    /** getOrComputeCacheKeyMd5 */
    public PUniqueId getOrComputeCacheKeyMd5() {
        if (cacheKeyMd5 == null && originSql != null) {
            synchronized (this) {
                if (cacheKeyMd5 != null) {
                    return cacheKeyMd5;
                }
                cacheKeyMd5 = doComputeCacheKeyMd5(usedVariables);
            }
        }
        return cacheKeyMd5;
    }

    /** doComputeCacheKeyMd5 */
    public synchronized PUniqueId doComputeCacheKeyMd5(Set<Variable> usedVariables) {
        StringBuilder cacheKey = new StringBuilder(originSql);
        for (Entry<FullTableName, String> entry : usedViews.entrySet()) {
            cacheKey.append("|")
                    .append(entry.getKey())
                    .append("=")
                    .append(entry.getValue());
        }
        for (Variable usedVariable : usedVariables) {
            cacheKey.append("|")
                    .append(usedVariable.getType().name())
                    .append(":")
                    .append(usedVariable.getName())
                    .append("=")
                    .append(usedVariable.getRealExpression().toSql());
        }
        for (Pair<Expression, Expression> pair : foldNondeterministicPairs) {
            cacheKey.append("|")
                    .append(pair.key().toSql())
                    .append("=")
                    .append(pair.value().toSql());
        }
        for (Entry<FullTableName, List<RowFilterPolicy>> entry : rowPolicies.entrySet()) {
            List<RowFilterPolicy> policy = entry.getValue();
            if (policy.isEmpty()) {
                continue;
            }
            cacheKey.append("|")
                    .append(entry.getKey())
                    .append("=")
                    .append(policy);
        }
        for (Entry<FullColumnName, Optional<DataMaskPolicy>> entry : dataMaskPolicies.entrySet()) {
            if (!entry.getValue().isPresent()) {
                continue;
            }
            cacheKey.append("|")
                    .append(entry.getKey())
                    .append("=")
                    .append(entry.getValue().map(Object::toString).orElse(""));
        }
        return CacheProxy.getMd5(cacheKey.toString());
    }

    public void setOriginSql(String originSql) {
        this.originSql = originSql.trim();
    }

    public Optional<ResultSet> getResultSetInFe() {
        return Optional.ofNullable(resultSetInFe);
    }

    public void setResultSetInFe(ResultSet resultSetInFe) {
        this.resultSetInFe = resultSetInFe;
    }

    public CacheKeyType getCacheKeyType() {
        return cacheKeyType;
    }

    public void setCacheKeyType(CacheKeyType cacheKeyType) {
        this.cacheKeyType = cacheKeyType;
    }

    /** FullTableName */
    @lombok.Data
    @lombok.AllArgsConstructor
    public static class FullTableName {
        public final String catalog;
        public final String db;
        public final String table;

        @Override
        public String toString() {
            return catalog + "." + db + "." + table;
        }
    }

    /** FullColumnName */
    @lombok.Data
    @lombok.AllArgsConstructor
    public static class FullColumnName {
        public final String catalog;
        public final String db;
        public final String table;
        public final String column;

        @Override
        public String toString() {
            return catalog + "." + db + "." + table + "." + column;
        }
    }

    /** ScanTable */
    @lombok.Data
    @lombok.AllArgsConstructor
    public static class ScanTable {
        public final FullTableName fullTableName;
        public final long latestVersion;
        public final List<Long> scanPartitions = Lists.newArrayList();

        public void addScanPartition(Long partitionId) {
            this.scanPartitions.add(partitionId);
        }
    }

    /** CacheKeyType */
    public enum CacheKeyType {
        // use `userIdentity`:`sql`.trim() as Cache key in FE
        SQL,
        // use MD5 as Cache key in FE
        MD5
    }
}
