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

package org.apache.doris.connector.iceberg.action;

import org.apache.doris.connector.api.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Pins the connector port of legacy {@code IcebergExecuteActionFactory} (the name registry + dispatch).
 *
 * <p><b>WHY this matters:</b> the supported-name list is exported to {@code getSupportedProcedures()} and
 * embedded in the unknown-procedure error, so its membership and order must match legacy byte-for-byte
 * (T08 byte-parity). The {@code table} parameter is dropped (it was always dead in legacy). The 9 switch
 * cases are added in T04 (the procedure bodies); T03 fixes the registry + the faithful unknown-procedure
 * rejection.</p>
 */
public class IcebergExecuteActionFactoryTest {

    @Test
    public void getSupportedActionsReturnsNineNamesInLegacyOrder() {
        Assertions.assertArrayEquals(
                new String[] {
                        "rollback_to_snapshot",
                        "rollback_to_timestamp",
                        "set_current_snapshot",
                        "cherrypick_snapshot",
                        "fast_forward",
                        "expire_snapshots",
                        "rewrite_data_files",
                        "publish_changes",
                        "rewrite_manifests",
                },
                IcebergExecuteActionFactory.getSupportedActions());
    }

    @Test
    public void createActionRejectsUnknownProcedureWithLegacyMessage() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergExecuteActionFactory.createAction(
                        "no_such_proc", Collections.emptyMap(), Collections.emptyList(), null));
        Assertions.assertEquals(
                "Unsupported Iceberg procedure: no_such_proc. Supported procedures: rollback_to_snapshot, "
                        + "rollback_to_timestamp, set_current_snapshot, cherrypick_snapshot, fast_forward, "
                        + "expire_snapshots, rewrite_data_files, publish_changes, rewrite_manifests",
                e.getMessage());
    }
}
