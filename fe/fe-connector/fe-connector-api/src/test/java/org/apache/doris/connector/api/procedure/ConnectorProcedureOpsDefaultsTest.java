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

package org.apache.doris.connector.api.procedure;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Pins the {@link Connector#getProcedureOps()} default and the {@link ConnectorProcedureResult} shape.
 *
 * <p><b>WHY this matters:</b> P6.4 adds the {@code getProcedureOps()} accessor so the engine can route
 * {@code ALTER TABLE EXECUTE} to a connector. The default MUST be {@code null} so every connector that
 * declares no table procedures (jdbc / es / maxcompute / paimon / trino) inherits the no-op and stays
 * behaviorally unchanged — only iceberg overrides it. A non-null default would make the engine attempt a
 * procedure dispatch on connectors that have none.</p>
 */
public class ConnectorProcedureOpsDefaultsTest {

    /** Minimal connector overriding only the single mandatory method, to read the inherited defaults. */
    private static final class BareConnector implements Connector {
        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return null;
        }
    }

    /** Minimal {@link ConnectorProcedureOps} overriding only the mandatory methods, to read the defaults. */
    private static final class BareProcedureOps implements ConnectorProcedureOps {
        @Override
        public List<String> getSupportedProcedures() {
            return Collections.emptyList();
        }

        @Override
        public ConnectorProcedureResult execute(ConnectorSession session, ConnectorTableHandle table,
                String procedureName, Map<String, String> properties, ConnectorPredicate whereCondition,
                List<String> partitionNames) {
            return null;
        }
    }

    @Test
    public void getProcedureOpsDefaultsToNull() {
        Assertions.assertNull(new BareConnector().getProcedureOps(),
                "a connector that declares no table procedures must inherit a null getProcedureOps() so "
                        + "ALTER TABLE EXECUTE is never dispatched to it (jdbc/es/maxcompute/paimon/trino "
                        + "stay behaviorally unchanged)");
    }

    @Test
    public void getExecutionModeDefaultsToSingleCall() {
        // A connector that declares only synchronous procedures inherits SINGLE_CALL for every name, so the
        // engine never attempts distributed orchestration on a procedure that has none. Only a connector with
        // a genuinely distributed procedure (iceberg rewrite_data_files) overrides this.
        ConnectorProcedureOps ops = new BareProcedureOps();
        Assertions.assertEquals(ProcedureExecutionMode.SINGLE_CALL,
                ops.getExecutionMode("any_procedure"),
                "the default execution mode must be SINGLE_CALL so the engine routes through execute()");
        Assertions.assertEquals(ProcedureExecutionMode.SINGLE_CALL,
                ops.getExecutionMode("rewrite_data_files"),
                "a connector that does not override getExecutionMode never reports DISTRIBUTED, even for a "
                        + "name another connector treats as distributed");
    }

    @Test
    public void planRewriteDefaultsToUnsupported() {
        ConnectorProcedureOps ops = new BareProcedureOps();
        // planRewrite is only meaningful for a DISTRIBUTED procedure; a connector that declares none must never
        // have it called (the engine checks getExecutionMode first). The default FAILS LOUD rather than
        // silently returning an empty plan (which would make a misrouted rewrite a no-op). MUTATION: defaulting
        // to `return Collections.emptyList()` -> no throw -> red.
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> ops.planRewrite(null, null, "rewrite_data_files",
                        Collections.emptyMap(), null, Collections.emptyList()));
    }

    @Test
    public void rewriteGroupExposesPathsAndStats() {
        ConnectorRewriteGroup g = new ConnectorRewriteGroup(
                Collections.singleton("oss://b/db/t1/f1.parquet"), 3, 4096L, 2);
        // The engine reads the raw paths (to scope each group's scan) and the per-group counts (to sum into the
        // result row), so all four must be carried verbatim. MUTATION: any getter returning a wrong field -> red.
        Assertions.assertEquals(Collections.singleton("oss://b/db/t1/f1.parquet"), g.getDataFilePaths());
        Assertions.assertEquals(3, g.getDataFileCount());
        Assertions.assertEquals(4096L, g.getTotalSizeBytes());
        Assertions.assertEquals(2, g.getDeleteFileCount());
    }

    @Test
    public void rewriteGroupRejectsNullPaths() {
        // Fail-loud construction: the engine scopes the scan by these paths, so a null set is a programming
        // error, not an empty scope.
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorRewriteGroup(null, 0, 0L, 0));
    }

    @Test
    public void procedureResultExposesSchemaAndRows() {
        ConnectorColumn col = new ConnectorColumn("current_snapshot_id", ConnectorType.of("BIGINT"),
                null, true, null);
        List<List<String>> rows = Collections.singletonList(Collections.singletonList("42"));
        ConnectorProcedureResult result = new ConnectorProcedureResult(Collections.singletonList(col), rows);

        Assertions.assertEquals(1, result.getResultSchema().size());
        Assertions.assertEquals("current_snapshot_id", result.getResultSchema().get(0).getName());
        Assertions.assertEquals(rows, result.getRows(),
                "rows are returned to the engine unchanged for result-set wrapping");
    }

    @Test
    public void procedureResultRejectsNullArgs() {
        // Fail-loud construction: the engine builds the result set from non-null schema + rows.
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorProcedureResult(null, Collections.emptyList()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorProcedureResult(Collections.emptyList(), null));
    }
}
