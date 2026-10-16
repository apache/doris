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

import org.apache.doris.connector.spi.DorisConnectorException;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Pins the Iceberg procedure name registry and dispatch.
 *
 * <p><b>WHY this matters:</b> the supported-name list is exported to {@code getSupportedProcedures()} and
 * embedded in the unknown-procedure error, so membership, ordering, and executable action mappings must stay
 * synchronized.</p>
 */
public class IcebergExecuteActionFactoryTest {

    @Test
    public void getSupportedActionsIncludesOrphanCleanup() {
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
                        "remove_orphan_files",
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
                        + "expire_snapshots, rewrite_data_files, publish_changes, rewrite_manifests, "
                        + "remove_orphan_files",
                e.getMessage());
    }

    @Test
    public void createRemoveOrphanFilesAction() {
        BaseIcebergAction action = IcebergExecuteActionFactory.createAction(
                "remove_orphan_files", Collections.singletonMap("older_than", "1"),
                Collections.emptyList(), null);
        Assertions.assertInstanceOf(IcebergRemoveOrphanFilesAction.class, action);
    }

    @Test
    public void removeOrphanFilesRejectsInvalidLocationUri() {
        BaseIcebergAction action = IcebergExecuteActionFactory.createAction(
                "remove_orphan_files", ImmutableMap.of("older_than", "1", "location", "://"),
                Collections.emptyList(), null);
        Assertions.assertThrows(DorisConnectorException.class, action::validate);
    }

    /**
     * CANARY for the dormant {@code rewrite_data_files} gap: it is advertised in {@link
     * IcebergExecuteActionFactory#getSupportedActions()} but has NO {@code createAction} switch
     * case because it is dispatched through the distributed rewrite planner, so it falls through to the
     * unknown-procedure rejection in this single-call factory.
     */
    @Test
    public void rewriteDataFilesIsAdvertisedButNotYetExecutable() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergExecuteActionFactory.createAction(
                        "rewrite_data_files", Collections.emptyMap(), Collections.emptyList(), null));
        Assertions.assertTrue(
                e.getMessage().startsWith("Unsupported Iceberg procedure: rewrite_data_files"),
                e.getMessage());
        Assertions.assertTrue(java.util.Arrays.asList(IcebergExecuteActionFactory.getSupportedActions())
                .contains("rewrite_data_files"));
    }
}
