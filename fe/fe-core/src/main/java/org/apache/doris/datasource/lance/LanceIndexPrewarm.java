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

package org.apache.doris.datasource.lance;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.lance.index.LanceIndexInspectionExecutor;
import org.apache.doris.datasource.lance.index.LanceIndexSegmentGroup;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.proto.InternalService.PLanceIndexPrewarmRequest;
import org.apache.doris.proto.InternalService.PLanceIndexPrewarmResponse;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

/** Pins table access and coordinates bounded synchronous prewarm RPCs. */
public final class LanceIndexPrewarm {
    static final int MAX_IN_FLIGHT = 8;
    private static final ShowResultSetMetaData RESULT_META = ShowResultSetMetaData.builder()
            .addColumn(new Column("Table", ScalarType.createStringType()))
            .addColumn(new Column("Index", ScalarType.createStringType()))
            .addColumn(new Column("DatasetVersion", ScalarType.BIGINT))
            .addColumn(new Column("BackendCount", ScalarType.INT))
            .addColumn(new Column("ElapsedMs", ScalarType.BIGINT)).build();

    private LanceIndexPrewarm() {
    }

    public static ShowResultSetMetaData getResultSetMetaData() {
        return RESULT_META;
    }

    public static void run(ConnectContext ctx, StmtExecutor executor, TableNameInfo tableName,
            List<String> columns, BooleanSupplier cancelled) throws Exception {
        long started = System.nanoTime();
        long elapsedMs = ctx.getStartTime() > 0 ? Math.max(0, System.currentTimeMillis() - ctx.getStartTime()) : 0;
        long remainingMs = TimeUnit.SECONDS.toMillis(ctx.getQueryTimeoutS()) - elapsedMs;
        long deadline = started + TimeUnit.MILLISECONDS.toNanos(Math.max(0, remainingMs));
        tableName.analyze(ctx);
        checkPrivileges(ctx, tableName);
        List<Backend> targets = selectBackends(resolveComputeGroup(ctx));
        // Resolve catalog/table initialization and snapshot IO in the bounded metadata pool.
        // The worker owns native resources until it returns, even if this caller stops waiting.
        String catalogName = tableName.getCtl();
        String databaseName = tableName.getDb();
        String name = tableName.getTbl();
        LanceTableMetadata metadata = LanceIndexInspectionExecutor.execute(() -> {
            CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
            if (!(catalog instanceof LanceExternalCatalog)) {
                throw new AnalysisException("Index-only WARM UP SELECT requires a Lance catalog table");
            }
            TableIf table = catalog.getDbOrAnalysisException(databaseName).getTableOrAnalysisException(name);
            if (!(table instanceof LanceExternalTable)) {
                throw new AnalysisException("Index-only WARM UP SELECT requires a Lance catalog table");
            }
            checkActive(deadline, cancelled);
            // Use the query access path, including REST-vended credentials, exactly once.
            return ((LanceExternalTable) table).loadMetadata();
        }, deadline, cancelled);
        List<String> indexes = selectIndexes(metadata, columns);
        List<List<String>> rows = new ArrayList<>();
        // All indexes share the resolved snapshot, target set and statement deadline.
        for (String index : indexes) {
            long indexStarted = System.nanoTime();
            PLanceIndexPrewarmRequest request = request(metadata, index);
            execute(targets, request, deadline, cancelled, (backend, rpcRequest, timeoutMs) ->
                    BackendServiceProxy.getInstance().prewarmLanceIndexAsync(
                            new TNetworkAddress(backend.getHost(), backend.getBrpcPort()), rpcRequest, timeoutMs));
            rows.add(Arrays.asList(tableName.toSql(), index, Long.toString(metadata.getVersion()),
                    Integer.toString(targets.size()),
                    Long.toString(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - indexStarted))));
        }
        executor.sendResultSet(new ShowResultSet(RESULT_META, rows));
    }

    static List<String> selectIndexes(LanceTableMetadata metadata, List<String> columns) throws AnalysisException {
        Set<Integer> fields = new HashSet<>();
        for (String column : columns) {
            List<String> matches = new ArrayList<>();
            metadata.getSchema().getFields().forEach(field -> {
                if (field.getName().equalsIgnoreCase(column)) {
                    matches.add(field.getName());
                }
            });
            if (matches.size() != 1) {
                throw new AnalysisException("Unknown or ambiguous index prewarm column: " + column);
            }
            OptionalInt field = metadata.getLanceFieldId(matches.get(0));
            if (!field.isPresent()) {
                throw new AnalysisException("Lance field ID is unavailable for index prewarm column: " + column);
            }
            fields.add(field.getAsInt());
        }
        List<String> indexes = new ArrayList<>();
        for (LanceIndexSegmentGroup index : metadata.getIndexes()) {
            if (columns.isEmpty() || index.getSegments().stream()
                    .anyMatch(segment -> segment.getFieldIds().stream().anyMatch(fields::contains))) {
                indexes.add(index.getName());
            }
        }
        if (indexes.isEmpty()) {
            throw new AnalysisException("No indexes match the WARM UP SELECT projection");
        }
        return indexes;
    }

    static void checkPrivileges(ConnectContext ctx, TableNameInfo table) throws AnalysisException {
        // Authorize before catalog lookup: resolving external metadata can perform remote IO.
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
            throw new AnalysisException("ADMIN privilege is required for Index-only WARM UP SELECT");
        }
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx,
                new TableName(table.getCtl(), table.getDb(), table.getTbl()), PrivPredicate.SELECT)) {
            throw new AnalysisException("SELECT denied for Index-only WARM UP SELECT on " + table.toSql());
        }
    }

    static ComputeGroup resolveComputeGroup(ConnectContext ctx) throws UserException {
        if (Config.isCloudMode()) {
            String selected = ctx.getCloudCluster();
            if (!Env.getCurrentEnv().getAccessManager().checkCloudPriv(ctx.getCurrentUserIdentity(),
                    selected, PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER)) {
                throw new AnalysisException("USAGE denied for compute group '" + selected + "'");
            }
            return Env.getCurrentEnv().getComputeGroupMgr().getComputeGroupByName(selected);
        }
        return ctx.getComputeGroup();
    }

    static List<Backend> selectBackends(ComputeGroup group) throws UserException {
        if (group == ComputeGroup.INVALID_COMPUTE_GROUP) {
            throw new AnalysisException(ComputeGroup.INVALID_COMPUTE_GROUP_ERR_MSG);
        }
        // Warm every eligible node, including all mixed nodes that an external scan may select
        // to supplement compute nodes. A randomized scheduling subset is insufficient here.
        List<Backend> targets = new BeSelectionPolicy.Builder().needQueryAvailable().needLoadAvailable()
                .preferComputeNode(Config.prefer_compute_node_for_external_table)
                .assignExpectBeNum(Integer.MAX_VALUE).build().getCandidateBackends(group.getBackendList());
        if (targets.isEmpty()) {
            throw new AnalysisException("No available backends for index prewarm in compute group '"
                    + group.getName() + "'");
        }
        return new ArrayList<>(targets);
    }

    static PLanceIndexPrewarmRequest request(LanceTableMetadata metadata, String indexName)
            throws AnalysisException {
        if (metadata.getVersion() <= 0) {
            throw new AnalysisException("Index prewarm requires a fixed positive dataset version");
        }
        if (indexName.isEmpty() || metadata.getIndexes().stream().noneMatch(i -> i.getName().equals(indexName))) {
            throw new AnalysisException("Lance index '" + indexName + "' does not exist in dataset version "
                    + metadata.getVersion());
        }
        return PLanceIndexPrewarmRequest.newBuilder().setDatasetUri(metadata.getDatasetUri())
                .setDatasetVersion(metadata.getVersion()).setIndexName(indexName)
                .putAllStorageOptions(metadata.getLanceStorageOptions()).build();
    }

    @FunctionalInterface
    interface RpcSender {
        Future<PLanceIndexPrewarmResponse> send(Backend backend, PLanceIndexPrewarmRequest request, long timeoutMs)
                throws Exception;
    }

    static void execute(List<Backend> targets, PLanceIndexPrewarmRequest request, long deadline,
            BooleanSupplier cancelled, RpcSender sender) throws UserException {
        if (targets.isEmpty()) {
            throw new AnalysisException("No target backends for index prewarm");
        }
        for (int offset = 0; offset < targets.size(); offset += MAX_IN_FLIGHT) {
            List<Future<PLanceIndexPrewarmResponse>> pending = new ArrayList<>();
            Backend current = targets.get(offset);
            try {
                int end = Math.min(targets.size(), offset + MAX_IN_FLIGHT);
                for (int i = offset; i < end; i++) {
                    current = targets.get(i);
                    long timeoutMs = checkActive(deadline, cancelled);
                    pending.add(sender.send(current, request.toBuilder().setTimeoutMs(timeoutMs).build(), timeoutMs));
                }
                for (int i = 0; i < pending.size(); i++) {
                    current = targets.get(offset + i);
                    PLanceIndexPrewarmResponse response;
                    while (true) {
                        long timeoutMs = checkActive(deadline, cancelled);
                        try {
                            response = pending.get(i).get(Math.min(timeoutMs, 100), TimeUnit.MILLISECONDS);
                            break;
                        } catch (TimeoutException e) {
                            // Poll connection/query cancellation without extending the statement deadline.
                        }
                    }
                    if (!response.hasStatus() || response.getStatus().getStatusCode() != TStatusCode.OK.getValue()) {
                        throw new UserException("Backend rejected or failed index prewarm");
                    }
                    if (!response.hasDatasetVersion() || response.getDatasetVersion() != request.getDatasetVersion()) {
                        throw new UserException("Backend did not acknowledge the pinned index prewarm snapshot");
                    }
                }
                checkActive(deadline, cancelled);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw failure(request, current, "interrupted");
            } catch (UserException e) {
                throw failure(request, current, e.getMessage());
            } catch (Exception e) {
                // RPC/provider exceptions may include request data. Old BEs return an RPC error
                // for the new method; neither that nor a missing success acknowledgement is OK.
                throw failure(request, current, "RPC failed or backend does not support index prewarm");
            } finally {
                pending.forEach(future -> future.cancel(true));
            }
        }
    }

    private static UserException failure(PLanceIndexPrewarmRequest request, Backend backend, String reason) {
        return new UserException("Lance index '" + request.getIndexName() + "', version "
                + request.getDatasetVersion() + ", backend " + backend.getId() + ": " + reason);
    }

    private static long checkActive(long deadline, BooleanSupplier cancelled) throws UserException {
        if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted()) {
            throw new UserException("Index prewarm cancelled");
        }
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new UserException("Index prewarm timed out");
        }
        return Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining));
    }
}
