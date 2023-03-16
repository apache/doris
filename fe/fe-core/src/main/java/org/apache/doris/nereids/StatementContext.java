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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.rules.analysis.ColumnAliasGenerator;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.GuardedBy;

/**
 * Statement context for nereids
 */
public class StatementContext {

    private ConnectContext connectContext;

    private OriginStatement originStatement;

    private int maxNAryInnerJoin = 0;

    private final IdGenerator<ExprId> exprIdGenerator = ExprId.createGenerator();

    private final IdGenerator<ObjectId> objectIdGenerator = ObjectId.createGenerator();

    @GuardedBy("this")
    private final Map<String, Supplier<Object>> contextCacheMap = Maps.newLinkedHashMap();

    private StatementBase parsedStatement;

    private Set<String> columnNames;

    private ColumnAliasGenerator columnAliasGenerator;

    public StatementContext() {
        this.connectContext = ConnectContext.get();
    }

    public StatementContext(ConnectContext connectContext, OriginStatement originStatement) {
        this.connectContext = connectContext;
        this.originStatement = originStatement;
    }

    public void setConnectContext(ConnectContext connectContext) {
        this.connectContext = connectContext;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public void setOriginStatement(OriginStatement originStatement) {
        this.originStatement = originStatement;
    }

    public OriginStatement getOriginStatement() {
        return originStatement;
    }

    public void setMaxNArayInnerJoin(int maxNAryInnerJoin) {
        if (maxNAryInnerJoin > this.maxNAryInnerJoin) {
            this.maxNAryInnerJoin = maxNAryInnerJoin;
        }
    }

    public int getMaxNAryInnerJoin() {
        return maxNAryInnerJoin;
    }

    public StatementBase getParsedStatement() {
        return parsedStatement;
    }

    public ExprId getNextExprId() {
        return exprIdGenerator.getNextId();
    }

    public ObjectId getNextObjectId() {
        return objectIdGenerator.getNextId();
    }

    public void setParsedStatement(StatementBase parsedStatement) {
        this.parsedStatement = parsedStatement;
    }

    /** getOrRegisterCache */
    public synchronized <T> T getOrRegisterCache(String key, Supplier<T> cacheSupplier) {
        Supplier<T> supplier = (Supplier<T>) contextCacheMap.get(key);
        if (supplier == null) {
            contextCacheMap.put(key, (Supplier<Object>) Suppliers.memoize(cacheSupplier));
            supplier = cacheSupplier;
        }
        return supplier.get();
    }

    public Set<String> getColumnNames() {
        return columnNames == null ? new HashSet<>() : columnNames;
    }

    public void setColumnNames(Set<String> columnNames) {
        this.columnNames = columnNames;
    }

    public ColumnAliasGenerator getColumnAliasGenerator() {
        return columnAliasGenerator == null
            ? columnAliasGenerator = new ColumnAliasGenerator(this)
            : columnAliasGenerator;
    }

    public String generateColumnName() {
        return getColumnAliasGenerator().getNextAlias();
    }
}
