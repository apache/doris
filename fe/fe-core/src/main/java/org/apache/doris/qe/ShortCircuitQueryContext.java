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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprList;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TSlotDescriptor;
import org.apache.doris.thrift.TTypeNodeType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public class ShortCircuitQueryContext {
    // BE Reusable::tuple_desc() consumes tuple 0. Auxiliary tuples, such as a full base-index
    // tuple created for an explicitly selected rollup, are not row-store decoding inputs.
    private static final int POINT_QUERY_TUPLE_ID = 0;
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
    public final OlapTable tbl;
    private final long fileCacheQueryLimitBytes;
    private final long partitionTopologyVersion;
    public final String tableName;
    private final boolean enablePruneNestedColumns;

    public final OlapScanNode scanNode;
    public final Queriable analzyedQuery;
    // Serialized mysql Field, this could avoid serialize mysql field each time sendFields.
    // Since, serialize fields is too heavy when table is wide
    Map<Integer, byte[]> serializedFields = Maps.newHashMap();

    List<Type> returnTypes = null;

    public byte[] getSerializedField(int idx) {
        return serializedFields.getOrDefault(idx, null);
    }

    public void addSerializedField(int idx, byte[] serializedField) {
        serializedFields.put(idx, serializedField);
    }

    List<Type> getReturnTypes() {
        if (returnTypes == null) {
            returnTypes = analzyedQuery.getResultExprs()
                    .stream().map(e -> e.getType()).collect(Collectors.toList());
        }
        return returnTypes;
    }

    public ShortCircuitQueryContext(Planner planner, Queriable analzyedQuery,
            SessionVariable effectiveSession) throws TException {
        this.planner = planner;
        TDescriptorTable descriptors = planner.getDescTable().toThrift();
        validateDescriptors(descriptors);
        this.serializedDescTable = ByteString.copyFrom(
                new TSerializer().serialize(descriptors));
        TQueryOptions options = planner.getQueryOptions() != null ? planner.getQueryOptions() : new TQueryOptions();
        // Capture effective planning options here, before StmtExecutor rolls back SET_VAR.
        this.enablePruneNestedColumns = effectiveSession.enablePruneNestedColumns;
        this.fileCacheQueryLimitBytes = options.isSetFileCacheQueryLimitBytes()
                ? options.getFileCacheQueryLimitBytes()
                : -1;
        this.serializedQueryOptions = ByteString.copyFrom(
                new TSerializer().serialize(options));
        List<TExpr> exprs = new ArrayList<>();
        OlapScanNode olapScanNode = (OlapScanNode) planner.getScanNodes().get(0);
        if (olapScanNode.getProjectList() != null) {
            // project on scan node
            exprs.addAll(olapScanNode.getProjectList().stream()
                    .map(Expr::treeToThrift).collect(Collectors.toList()));
        } else {
            // add output slots
            exprs.addAll(planner.getFragments().get(0).getOutputExprs().stream()
                    .map(Expr::treeToThrift).collect(Collectors.toList()));
        }
        TExprList exprList = new TExprList(exprs);
        serializedOutputExpr = ByteString.copyFrom(
                new TSerializer().serialize(exprList));
        this.cacheID = UUID.randomUUID();
        this.scanNode = olapScanNode;
        this.tbl = this.scanNode.getOlapTable();
        this.tableName = this.scanNode.getTableNameInPlan();
        this.schemaVersion = this.tbl.getBaseSchemaVersion();
        this.partitionTopologyVersion = this.tbl.getPartitionTopologyVersion();
        this.analzyedQuery = analzyedQuery;
    }

    @VisibleForTesting
    ShortCircuitQueryContext(OlapTable tbl, int schemaVersion, long fileCacheQueryLimitBytes) {
        this.planner = null;
        this.serializedDescTable = ByteString.EMPTY;
        this.serializedOutputExpr = ByteString.EMPTY;
        this.serializedQueryOptions = ByteString.EMPTY;
        this.cacheID = UUID.randomUUID();
        this.tbl = tbl;
        this.tableName = tbl.getName();
        this.schemaVersion = schemaVersion;
        this.fileCacheQueryLimitBytes = fileCacheQueryLimitBytes;
        this.partitionTopologyVersion = tbl.getPartitionTopologyVersion();
        this.enablePruneNestedColumns = new SessionVariable().enablePruneNestedColumns;
        this.scanNode = null;
        this.analzyedQuery = null;
    }

    public boolean isReusable(ConnectContext ctx) {
        return ctx.getSessionVariable().isEnableShortCircuitQuery()
                && this.enablePruneNestedColumns == ctx.getSessionVariable().enablePruneNestedColumns
                && !this.tbl.isDropped && Objects.equals(this.tableName, this.tbl.getName())
                && this.tbl.getBaseSchemaVersion() == this.schemaVersion
                && this.fileCacheQueryLimitBytes == ctx.getSessionVariable().fileCacheQueryLimitBytes
                && this.tbl.getPartitionTopologyVersion() == this.partitionTopologyVersion;
    }

    @VisibleForTesting
    static void validateDescriptors(TDescriptorTable descriptors) throws TException {
        if (descriptors.isSetSlotDescriptors()) {
            for (TSlotDescriptor slot : descriptors.getSlotDescriptors()) {
                if (slot.getParent() != POINT_QUERY_TUPLE_ID) {
                    continue;
                }
                if ((slot.isSetAllAccessPaths() && !slot.getAllAccessPaths().isEmpty())
                        || (slot.isSetPredicateAccessPaths() && !slot.getPredicateAccessPaths().isEmpty())
                        || (slot.isSetColumnPaths() && !slot.getColumnPaths().isEmpty())
                        || slot.getSlotType().getTypesSize() != 1
                        || slot.getSlotType().getTypes().get(0).getType() != TTypeNodeType.SCALAR
                        || slot.getSlotType().getTypes().get(0).getScalarType().getType() == TPrimitiveType.VARIANT) {
                    throw new TException("Short-circuit point queries require scalar slots without access paths");
                }
            }
        }
    }

    public void sanitize() {
        Preconditions.checkNotNull(serializedDescTable);
        Preconditions.checkNotNull(serializedOutputExpr);
        Preconditions.checkNotNull(cacheID);
        Preconditions.checkNotNull(tbl);
        Preconditions.checkNotNull(tableName);
    }
}
