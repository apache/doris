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

package org.apache.doris.nereids.trees.plans.commands.execute;

import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.procedure.ConnectorProcedureOps;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;
import org.apache.doris.connector.api.procedure.ConnectorRewriteGroup;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Guards the engine-neutral parts of {@link ConnectorRewriteDriver} that are unit-testable without a live
 * cluster: the empty-plan early return (no transaction opened, all-zero row) and the connector-failure
 * mapping. The full distributed write path (N INSERT-SELECTs against BE) is exercised at the flip rehearsal.
 */
public class ConnectorRewriteDriverTest {

    private ConnectorRewriteDriver driverWith(ConnectorProcedureOps procedureOps, ConnectorMetadata metadata) {
        return driverWith(procedureOps, metadata, null);
    }

    private ConnectorRewriteDriver driverWith(ConnectorProcedureOps procedureOps, ConnectorMetadata metadata,
            ConnectorPredicate where) {
        return new ConnectorRewriteDriver(
                Mockito.mock(ConnectContext.class),
                Mockito.mock(ExternalTable.class),
                Mockito.mock(PluginDrivenExternalCatalog.class),
                metadata,
                procedureOps,
                Mockito.mock(ConnectorSession.class),
                Mockito.mock(ConnectorTableHandle.class),
                "rewrite_data_files",
                Collections.emptyMap(),
                Collections.emptyList(),
                where);
    }

    @Test
    public void emptyPlanReturnsZeroRowWithoutOpeningTransaction() throws Exception {
        ConnectorProcedureOps procedureOps = Mockito.mock(ConnectorProcedureOps.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(procedureOps.planRewrite(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenReturn(Collections.emptyList());

        ConnectorProcedureResult result = driverWith(procedureOps, metadata).run();

        // Four-column schema in the exact legacy order and types.
        List<ConnectorColumn> schema = result.getResultSchema();
        Assertions.assertEquals(Arrays.asList(
                "rewritten_data_files_count", "added_data_files_count",
                "rewritten_bytes_count", "removed_delete_files_count"),
                schema.stream().map(ConnectorColumn::getName).collect(Collectors.toList()));
        Assertions.assertEquals(Arrays.asList("INT", "INT", "INT", "BIGINT"),
                schema.stream().map(c -> c.getType().getTypeName()).collect(Collectors.toList()));
        // Single all-zero row: nothing to rewrite.
        Assertions.assertEquals(Collections.singletonList(Arrays.asList("0", "0", "0", "0")), result.getRows());
        // MUTATION: dropping the empty-groups early return is killed — no transaction may be opened, and no
        // group work scheduled, when there is nothing to rewrite. The driver opens the txn via the per-handle
        // beginTransaction(session, handle) overload, so watch that one (the single-arg matcher would go
        // vacuous once the call site passes the resolved tableHandle).
        Mockito.verify(metadata, Mockito.never()).beginTransaction(Mockito.any(), Mockito.any());
    }

    @Test
    public void whereConditionIsThreadedToPlanRewrite() throws Exception {
        // The lowered WHERE must reach the connector's planRewrite as the 5th argument (the file-scope filter),
        // not be dropped to null. MUTATION: passing null instead of whereCondition is killed here.
        ConnectorProcedureOps procedureOps = Mockito.mock(ConnectorProcedureOps.class);
        Mockito.when(procedureOps.planRewrite(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenReturn(Collections.emptyList());
        ConnectorPredicate where = new ConnectorPredicate(new ConnectorColumnRef("a", ConnectorType.of("INT")));

        driverWith(procedureOps, Mockito.mock(ConnectorMetadata.class), where).run();

        ArgumentCaptor<ConnectorPredicate> captor = ArgumentCaptor.forClass(ConnectorPredicate.class);
        Mockito.verify(procedureOps).planRewrite(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                captor.capture(), Mockito.any());
        Assertions.assertSame(where, captor.getValue(), "the driver must pass the lowered WHERE through verbatim");
    }

    @Test
    public void planRewriteFailureSurfacesAsUserException() {
        ConnectorProcedureOps procedureOps = Mockito.mock(ConnectorProcedureOps.class);
        Mockito.when(procedureOps.planRewrite(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any())).thenThrow(new DorisConnectorException("plan boom"));

        ConnectorRewriteDriver driver = driverWith(procedureOps, Mockito.mock(ConnectorMetadata.class));
        UserException ex = Assertions.assertThrows(UserException.class, driver::run);
        Assertions.assertTrue(ex.getMessage().contains("plan boom"),
                "the connector failure text must be preserved, got: " + ex.getMessage());
    }

    @Test
    public void unionSourceFilePathsMergesAllGroupsAndDedupsByPath() {
        // STEP 3 registers the UNION of every group's source files in ONE connector call (one planFiles() scan)
        // instead of one call per group. Disjoint groups union straight; a path recurring across groups collapses
        // to a single entry, so the connector never double-registers a file to delete. MUTATION: unioning only
        // the first group (or not deduping) is killed here.
        ConnectorRewriteGroup g1 = new ConnectorRewriteGroup(
                ImmutableSet.of("s3://b/t/a.parquet", "s3://b/t/b.parquet"), 2, 2048L, 0);
        ConnectorRewriteGroup g2 = new ConnectorRewriteGroup(
                ImmutableSet.of("s3://b/t/c.parquet"), 1, 1024L, 0);
        // Defensive: a path shared with g1 (bin-packing keeps groups disjoint, but the union must still dedup).
        ConnectorRewriteGroup g3 = new ConnectorRewriteGroup(
                ImmutableSet.of("s3://b/t/a.parquet", "s3://b/t/d.parquet"), 2, 2048L, 0);

        Set<String> union = ConnectorRewriteDriver.unionSourceFilePaths(Arrays.asList(g1, g2, g3));

        Assertions.assertEquals(
                ImmutableSet.of("s3://b/t/a.parquet", "s3://b/t/b.parquet", "s3://b/t/c.parquet",
                        "s3://b/t/d.parquet"),
                union, "the union must contain each distinct source path exactly once across all groups");
    }

    @Test
    public void unionSourceFilePathsSkipsEmptyGroupsAndEmptyPlan() {
        // An empty group contributes nothing; an all-empty plan unions to the empty set (the connector treats
        // that as a no-op registration — the same net state as the former loop making N early-returning calls).
        ConnectorRewriteGroup withFiles = new ConnectorRewriteGroup(
                ImmutableSet.of("s3://b/t/a.parquet"), 1, 1024L, 0);
        ConnectorRewriteGroup empty = new ConnectorRewriteGroup(Collections.emptySet(), 0, 0L, 0);

        Assertions.assertEquals(ImmutableSet.of("s3://b/t/a.parquet"),
                ConnectorRewriteDriver.unionSourceFilePaths(Arrays.asList(withFiles, empty)),
                "an empty group must not affect the union");
        Assertions.assertTrue(
                ConnectorRewriteDriver.unionSourceFilePaths(Collections.emptyList()).isEmpty(),
                "an all-empty plan unions to the empty set");
    }
}
