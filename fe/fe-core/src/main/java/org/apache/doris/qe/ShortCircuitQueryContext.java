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

package org.apache.doris.qe;

import org.apache.doris.analysis.DescriptorToThriftConverter;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprList;
import org.apache.doris.thrift.TQueryOptions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public class ShortCircuitQueryContext {
    // Cached for better CPU performance, since serialize DescriptorTable and
    // outputExprs are heavy work
    public final Planner planner;
    public final ByteString serializedDescTable;
    public final ByteString serializedOutputExpr;
    public final ByteString serializedQueryOptions;

    // For prepared statement cached structure,
    // there are some pre-calculated structure in Backend TabletFetch service
    // using this ID to find for this prepared statement
    public final UUID cacheID;

    public final int schemaVersion;
    private final long baseIndexId;
    private final boolean showHiddenColumns;
    private boolean preparedPointQueryBindings;
    private List<Column> keyColumns;
    private PointQueryBindings pointQueryBindings;
    private List<Column> bindingKeyColumns;
    private Expr[] bindingConjuncts;
    private Class<?>[] bindingChildClasses;
    public final OlapTable tbl;
    public final String tableName;
    private final long fileCacheQueryLimitBytes;
    private final long partitionTopologyVersion;

    public final OlapScanNode scanNode;
    public final Queriable analzyedQuery;
    // Serialized mysql Field, this could avoid serialize mysql field each time sendFields.
    // Since, serialize fields is too heavy when table is wide
    private byte[][] serializedFields;

    List<Type> returnTypes = null;

    public byte[][] getOrCreateSerializedFields(int count) {
        if (serializedFields == null || serializedFields.length != count) {
            serializedFields = new byte[count][];
        }
        return serializedFields;
    }

    List<Type> getReturnTypes() {
        if (returnTypes == null) {
            returnTypes = analzyedQuery.getResultExprs()
                    .stream().map(e -> e.getType()).collect(Collectors.toList());
        }
        return returnTypes;
    }

    public ShortCircuitQueryContext(Planner planner, Queriable analzyedQuery) throws TException {
        this.planner = planner;
        this.serializedDescTable = ByteString.copyFrom(
                new TSerializer().serialize(DescriptorToThriftConverter.toThrift(planner.getDescTable())));
        TQueryOptions options = planner.getQueryOptions() != null ? planner.getQueryOptions() : new TQueryOptions();
        this.fileCacheQueryLimitBytes = options.isSetFileCacheQueryLimitBytes()
                ? options.getFileCacheQueryLimitBytes()
                : -1;
        this.serializedQueryOptions = ByteString.copyFrom(
                new TSerializer().serialize(options));
        List<TExpr> exprs = new ArrayList<>();
        OlapScanNode olapScanNode = (OlapScanNode) planner.getScanNodes().get(0);
        List<Expr> pointQueryProjectList = olapScanNode.getPointQueryProjectList();
        if (pointQueryProjectList != null) {
            // project on scan node
            exprs.addAll(pointQueryProjectList.stream()
                    .map(ExprToThriftVisitor::treeToThrift).collect(Collectors.toList()));
        } else {
            // add output slots
            exprs.addAll(planner.getFragments().get(0).getOutputExprs().stream()
                    .map(ExprToThriftVisitor::treeToThrift).collect(Collectors.toList()));
        }
        TExprList exprList = new TExprList(exprs);
        serializedOutputExpr = ByteString.copyFrom(
                new TSerializer().serialize(exprList));
        this.cacheID = UUID.randomUUID();
        this.scanNode = olapScanNode;
        this.tbl = this.scanNode.getOlapTable();
        this.tableName = this.scanNode.getTableNameInPlan();
        this.schemaVersion = this.tbl.getBaseSchemaVersion();
        this.baseIndexId = tbl.getBaseIndexId();
        this.showHiddenColumns = Util.showHiddenColumns();
        this.partitionTopologyVersion = this.tbl.getPartitionTopologyVersion();
        this.analzyedQuery = analzyedQuery;
    }

    @VisibleForTesting
    ShortCircuitQueryContext(OlapTable tbl, String tableName, int schemaVersion,
            long fileCacheQueryLimitBytes) {
        this.planner = null;
        this.serializedDescTable = ByteString.EMPTY;
        this.serializedOutputExpr = ByteString.EMPTY;
        this.serializedQueryOptions = ByteString.EMPTY;
        this.cacheID = UUID.randomUUID();
        this.tbl = tbl;
        this.tableName = tableName;
        this.schemaVersion = schemaVersion;
        this.fileCacheQueryLimitBytes = fileCacheQueryLimitBytes;
        this.baseIndexId = tbl.getBaseIndexId();
        this.showHiddenColumns = Util.showHiddenColumns();
        this.partitionTopologyVersion = tbl.getPartitionTopologyVersion();
        this.scanNode = null;
        this.analzyedQuery = null;
    }

    // Only ExecuteCommand's cached Nereids prepared plan opts into these structural caches.
    public void enablePreparedPointQueryBindings() {
        if (!preparedPointQueryBindings) {
            keyColumns = Collections.unmodifiableList(tbl.getBaseSchemaKeyColumns());
            preparedPointQueryBindings = true;
        }
    }

    private boolean usePreparedPointQueryBindings() {
        ConnectContext context = ConnectContext.get();
        return preparedPointQueryBindings && context != null
                && context.getCommand() == MysqlCommand.COM_STMT_EXECUTE;
    }

    public boolean isReusable(ConnectContext ctx) {
        return !this.tbl.isDropped
                && this.tbl.getBaseSchemaVersion() == this.schemaVersion
                && this.tbl.getBaseIndexId() == this.baseIndexId
                && Util.showHiddenColumns() == this.showHiddenColumns
                && Objects.equals(this.tableName, this.tbl.getName())
                && this.fileCacheQueryLimitBytes == ctx.getSessionVariable().fileCacheQueryLimitBytes
                && this.tbl.getPartitionTopologyVersion() == this.partitionTopologyVersion;
    }

    // Cache only schema structure. Routing and parameter values are recomputed for every execution.
    List<Column> getKeyColumns() {
        if (usePreparedPointQueryBindings() && keyColumns != null && isReusable(ConnectContext.get())) {
            return keyColumns;
        }
        return scanNode.getOlapTable().getBaseSchemaKeyColumns();
    }

    PointQueryBindings getPointQueryBindings() {
        if (!usePreparedPointQueryBindings()) {
            return null;
        }
        List<Column> columns = getKeyColumns();
        List<Expr> conjuncts = scanNode.getConjuncts();
        boolean matches = pointQueryBindings != null
                ? pointQueryBindings.matches(conjuncts) : sameBindingConjuncts(conjuncts);
        if (bindingKeyColumns != columns || !matches) {
            pointQueryBindings = PointQueryBindings.create(columns, conjuncts);
            bindingKeyColumns = columns;
            // Remember unsupported layouts too; their existing fallback must not rebuild this map each time.
            if (pointQueryBindings == null) {
                bindingConjuncts = conjuncts.toArray(new Expr[0]);
                bindingChildClasses = new Class<?>[conjuncts.size() * 2];
                for (int i = 0; i < conjuncts.size(); i++) {
                    bindingChildClasses[i * 2] = bindingChildClass(conjuncts.get(i), 0);
                    bindingChildClasses[i * 2 + 1] = bindingChildClass(conjuncts.get(i), 1);
                }
            } else {
                bindingConjuncts = null;
                bindingChildClasses = null;
            }
        }
        return pointQueryBindings;
    }

    private boolean sameBindingConjuncts(List<Expr> conjuncts) {
        if (bindingConjuncts == null || bindingConjuncts.length != conjuncts.size()) {
            return false;
        }
        for (int i = 0; i < bindingConjuncts.length; i++) {
            if (bindingConjuncts[i] != conjuncts.get(i)
                    || bindingChildClasses[i * 2] != bindingChildClass(conjuncts.get(i), 0)
                    || bindingChildClasses[i * 2 + 1] != bindingChildClass(conjuncts.get(i), 1)) {
                return false;
            }
        }
        return true;
    }

    private static Class<?> bindingChildClass(Expr expr, int ordinal) {
        return expr.getChildren().size() == 2 ? expr.getChild(ordinal).getClass() : null;
    }

    public void sanitize() {
        Preconditions.checkNotNull(serializedDescTable);
        Preconditions.checkNotNull(serializedOutputExpr);
        Preconditions.checkNotNull(cacheID);
        Preconditions.checkNotNull(tbl);
        Preconditions.checkNotNull(tableName);
    }
}
