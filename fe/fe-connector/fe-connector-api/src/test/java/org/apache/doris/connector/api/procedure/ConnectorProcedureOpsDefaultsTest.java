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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

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

    @Test
    public void getProcedureOpsDefaultsToNull() {
        Assertions.assertNull(new BareConnector().getProcedureOps(),
                "a connector that declares no table procedures must inherit a null getProcedureOps() so "
                        + "ALTER TABLE EXECUTE is never dispatched to it (jdbc/es/maxcompute/paimon/trino "
                        + "stay behaviorally unchanged)");
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
