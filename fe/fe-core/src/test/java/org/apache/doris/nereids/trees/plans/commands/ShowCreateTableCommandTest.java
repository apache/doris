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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.PluginDrivenSysExternalTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Guards the F4/F13 fix in {@link ShowCreateTableCommand#doRun}: a system table ($snapshots/...) must be
 * redirected to its source base table before {@code Env.getDdlStmt} renders it, so SHOW CREATE emits the base
 * table's DDL rather than the sys-table shell. Post-cutover a sys table is a {@link PluginDrivenSysExternalTable}
 * (the legacy doRun only unwrapped {@link org.apache.doris.datasource.iceberg.IcebergSysExternalTable}); the
 * redirect was already present in {@code validate()} for the privilege check, so doRun's omission was
 * asymmetric.
 */
public class ShowCreateTableCommandTest {

    @Test
    public void redirectSysTableToSourceUnwrapsPluginSysTable() {
        // MUTATION: dropping the PluginDrivenSysExternalTable arm -> the sys shell flows to Env.getDdlStmt
        // (sys-table name/columns, PARTITION BY suppressed) instead of the base table DDL -> red.
        PluginDrivenExternalTable source = Mockito.mock(PluginDrivenExternalTable.class);
        PluginDrivenSysExternalTable sys = Mockito.mock(PluginDrivenSysExternalTable.class);
        Mockito.when(sys.getSourceTable()).thenReturn(source);

        Assertions.assertSame(source, ShowCreateTableCommand.redirectSysTableToSource(sys),
                "a PluginDrivenSysExternalTable must be redirected to its source base table");
    }

    @Test
    public void redirectSysTableToSourcePassesThroughPlainTable() {
        // A non-sys table must pass through unchanged (no accidental unwrap / ClassCast). MUTATION: an
        // unconditional getSourceTable() cast would break for a plain table -> red.
        TableIf plain = Mockito.mock(TableIf.class);
        Assertions.assertSame(plain, ShowCreateTableCommand.redirectSysTableToSource(plain));
    }
}
