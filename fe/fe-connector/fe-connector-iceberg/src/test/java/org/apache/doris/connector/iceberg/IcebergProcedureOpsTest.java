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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.DorisConnectorException;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Pins the {@link IcebergProcedureOps} dispatch skeleton (P6.4-T03).
 *
 * <p><b>WHY this matters:</b> T03 turns the dormant T02 stub into a real seam — {@code getSupportedProcedures()}
 * now exports the factory's name list, and {@code execute()} routes through the factory. The 9 procedure
 * bodies land in T04, so a known name is not yet executable here; what T03 fixes is the discovery list and
 * the faithful unknown-procedure rejection, reached before any catalog/auth work (so no SDK table is
 * needed). The whole path stays dormant pre-cutover (iceberg is not {@code PluginDrivenExternalTable} until
 * P6.6).</p>
 */
public class IcebergProcedureOpsTest {

    private static IcebergProcedureOps newOps() {
        // getSupportedProcedures + the unknown-name rejection never touch the catalog/context, so they may
        // be null here; the catalog-backed path is exercised in T04 with an InMemoryCatalog.
        return new IcebergProcedureOps(Collections.emptyMap(), null, null);
    }

    @Test
    public void getSupportedProceduresExportsFactoryNamesInLegacyOrder() {
        Assertions.assertEquals(
                ImmutableList.of(
                        "rollback_to_snapshot",
                        "rollback_to_timestamp",
                        "set_current_snapshot",
                        "cherrypick_snapshot",
                        "fast_forward",
                        "expire_snapshots",
                        "rewrite_data_files",
                        "publish_changes",
                        "rewrite_manifests"),
                newOps().getSupportedProcedures());
    }

    @Test
    public void executeRejectsUnknownProcedureWithLegacyMessage() {
        IcebergTableHandle handle = new IcebergTableHandle("db", "tbl");
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> newOps().execute(null, handle, "no_such_proc",
                        Collections.emptyMap(), null, Collections.emptyList()));
        Assertions.assertEquals(
                "Unsupported Iceberg procedure: no_such_proc. Supported procedures: rollback_to_snapshot, "
                        + "rollback_to_timestamp, set_current_snapshot, cherrypick_snapshot, fast_forward, "
                        + "expire_snapshots, rewrite_data_files, publish_changes, rewrite_manifests",
                e.getMessage());
    }
}
