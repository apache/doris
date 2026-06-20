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

package org.apache.doris.datasource.systable;

import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.PluginDrivenSysExternalTable;

/**
 * Generic {@link NativeSysTable} for plugin-driven connectors.
 *
 * <p>Unlike {@code PaimonSysTable} (which enumerates a fixed connector-specific set), instances of this
 * class are created on demand by {@link PluginDrivenExternalTable#getSupportedSysTables()} from the
 * names the connector SPI reports. {@link #createSysExternalTable(ExternalTable)} builds the transient
 * {@link PluginDrivenSysExternalTable} that the planner executes through the native table path.</p>
 */
public class PluginDrivenSysTable extends NativeSysTable {

    public PluginDrivenSysTable(String sysName) {
        super(sysName);
    }

    @Override
    public ExternalTable createSysExternalTable(ExternalTable sourceTable) {
        if (!(sourceTable instanceof PluginDrivenExternalTable)) {
            throw new IllegalArgumentException(
                    "Expected PluginDrivenExternalTable but got " + sourceTable.getClass().getSimpleName());
        }
        return new PluginDrivenSysExternalTable((PluginDrivenExternalTable) sourceTable, getSysTableName());
    }
}
