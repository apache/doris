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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.StorageVault;
import org.apache.doris.catalog.StorageVault.StorageVaultType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Map;

/**
 * Alter Storage Vault command
 */
public class AlterStorageVaultCommand extends Command implements ForwardWithSync {
    private static final String TYPE = "type";
    private final Map<String, String> properties;
    private final String name;

    public AlterStorageVaultCommand(String name, final Map<String, String> properties) {
        super(PlanType.ALTER_STORAGE_VAULT);
        this.name = name;
        this.properties = properties;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        StorageVault.StorageVaultType vaultType = StorageVaultType.fromString(properties.get(TYPE));
        if (vaultType == StorageVault.StorageVaultType.UNKNOWN) {
            throw new AnalysisException("Unsupported Storage Vault type: " + type);
        }
        Env.getCurrentEnv().getStorageVaultMgr().alterStorageVault(vaultType, properties, name);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }
}
