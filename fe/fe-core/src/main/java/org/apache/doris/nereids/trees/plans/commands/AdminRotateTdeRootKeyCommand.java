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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.encryption.KeyManagerInterface;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Map;
import java.util.Objects;

/**
 * Rotate TDE root key command
 */
public class AdminRotateTdeRootKeyCommand extends Command implements ForwardWithSync {

    public static final String DORIS_TDE_KEY_PROVIDER = "doris_tde_key_provider";

    public static final String DORIS_TDE_KEY_PASSWORD = "doris_tde_key_password";

    public static final String DORIS_TDE_KEY_ORIGINAL_PASSWORD = "doris_tde_key_original_password";

    public static final String DORIS_TDE_KEY_ID = "doris_tde_key_id";

    public static final String DORIS_TDE_KEY_ENDPOINT = "doris_tde_key_endpoint";

    public static final String DORIS_TDE_KEY_REGION = "doris_tde_key_region";

    private final Map<String, String> properties;

    public AdminRotateTdeRootKeyCommand(Map<String, String> properties) {
        super(PlanType.ADMIN_ROTATE_TDE_ROOT_KEY);
        Objects.requireNonNull(properties, "properties are required");
        this.properties = properties;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        KeyManagerInterface keyManager = ctx.getEnv().getKeyManager();
        if (keyManager != null) {
            keyManager.rotateRootKey(properties);
        } else {
            throw new AnalysisException("TDE is disabled, cannot rotate root key");
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }
}
