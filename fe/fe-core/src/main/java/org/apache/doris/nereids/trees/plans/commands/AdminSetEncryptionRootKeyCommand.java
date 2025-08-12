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
import org.apache.doris.encryption.EncryptionKey;
import org.apache.doris.encryption.RootKeyInfo;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;

/**
 * admin set encryption root key command
 */
public class AdminSetEncryptionRootKeyCommand extends Command implements ForwardWithSync {
    public static final String PROPERTIES_TYPE = "type";
    public static final String PROPERTIES_ENCRYPTION_ALGORITHM = "encryption_algorithm";
    public static final String PROPERTIES_REGION = "region";
    public static final String PROPERTIES_CMK_ID = "cmk_id";
    public static final String PROPERTIES_AK = "ak";
    public static final String PROPERTIES_SK = "sk";

    private static final Logger LOG = LogManager.getLogger(AdminSetEncryptionRootKeyCommand.class);
    private final Map<String, String> properties;
    private final RootKeyInfo rootKeyInfo = new RootKeyInfo();

    public AdminSetEncryptionRootKeyCommand(Map<String, String> properties) {
        super(PlanType.ADMIN_SET_ENCRYPTION_ROOT_KEY_COMMAND);
        Objects.requireNonNull(properties, "properties is null");
        this.properties = properties;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        ctx.getEnv().getKeyManager().setRootKey(rootKeyInfo);
    }

    /**
     * validate
     */
    public void validate() throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("The properties must not be empty");
        }

        String typeValue = properties.get(PROPERTIES_TYPE);
        if (Strings.isNullOrEmpty(typeValue)) {
            throw new AnalysisException("The type field cannot be empty.");
        }
        try {
            rootKeyInfo.type = RootKeyInfo.RootKeyType.valueOf(typeValue.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("invalid root key type: " + typeValue);
        }

        String encryptionAlgorithmValue = properties.get(PROPERTIES_ENCRYPTION_ALGORITHM);
        if (Strings.isNullOrEmpty(encryptionAlgorithmValue)) {
            throw new AnalysisException("The encryption_algorithm field cannot be empty.");
        }
        try {
            rootKeyInfo.algorithm = EncryptionKey.Algorithm.valueOf(encryptionAlgorithmValue.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("invalid encryption algorithm: " + encryptionAlgorithmValue);
        }

        rootKeyInfo.region = properties.get(PROPERTIES_REGION);
        if (Strings.isNullOrEmpty(rootKeyInfo.region)) {
            throw new AnalysisException("The region field cannot be empty.");
        }

        rootKeyInfo.cmkId = properties.get(PROPERTIES_CMK_ID);
        if (Strings.isNullOrEmpty(rootKeyInfo.cmkId)) {
            throw new AnalysisException("The cmk_id field cannot be empty.");
        }

        rootKeyInfo.ak = properties.get(PROPERTIES_AK);
        rootKeyInfo.sk = properties.get(PROPERTIES_SK);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }
}
