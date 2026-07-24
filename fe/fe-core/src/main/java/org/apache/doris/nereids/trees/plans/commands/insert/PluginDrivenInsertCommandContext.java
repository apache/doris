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

package org.apache.doris.nereids.trees.plans.commands.insert;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Insert command context for plugin-driven connector catalogs.
 *
 * <p>{@code overwrite} is inherited from {@link BaseExternalTableInsertCommandContext}.
 * The static partition spec — a generic {@code col -> val} map — is carried here and
 * handed to the connector via the write context of
 * {@code ConnectorWritePlanProvider.planWrite}. It is populated during sink binding
 * (wired at the connector cutover) and defaults to empty, so a write with no static
 * partition contributes nothing to partition pinning.</p>
 *
 * <p>{@code branchName} carries the {@code INSERT INTO t@branch(name)} target. It is threaded onto
 * the connector write handle ({@code ConnectorWriteHandle.getBranchName}) so a versioned-table
 * connector points the commit at the branch; empty (the default) means the table's default ref.</p>
 */
public class PluginDrivenInsertCommandContext extends BaseExternalTableInsertCommandContext {

    private Map<String, String> staticPartitionSpec = Collections.emptyMap();
    private Optional<String> branchName = Optional.empty();

    public Map<String, String> getStaticPartitionSpec() {
        return staticPartitionSpec;
    }

    public void setStaticPartitionSpec(Map<String, String> staticPartitionSpec) {
        this.staticPartitionSpec =
                staticPartitionSpec == null ? Collections.emptyMap() : staticPartitionSpec;
    }

    public Optional<String> getBranchName() {
        return branchName;
    }

    public void setBranchName(Optional<String> branchName) {
        this.branchName = branchName == null ? Optional.empty() : branchName;
    }
}
