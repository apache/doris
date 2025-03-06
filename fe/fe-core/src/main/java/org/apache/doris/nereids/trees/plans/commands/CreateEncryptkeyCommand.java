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

import org.apache.doris.analysis.EncryptKeyName;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.EncryptKey;
import org.apache.doris.catalog.EncryptKeyHelper;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;

/** CreateEncryptkeyCommand */
public class CreateEncryptkeyCommand extends Command implements ForwardWithSync {
    private final boolean ifNotExists;
    private final EncryptKeyName encryptKeyName;
    private final String keyString;

    public CreateEncryptkeyCommand(EncryptKeyName encryptKeyName, boolean ifNotExists, String keyString) {
        super(PlanType.CREATE_ENCRYPTKEY_COMMAND);
        this.ifNotExists = ifNotExists;
        this.encryptKeyName = encryptKeyName;
        this.keyString = keyString;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check operation privilege
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        encryptKeyName.analyze(ctx);
        if (Strings.isNullOrEmpty(keyString)) {
            throw new AnalysisException("keyString can not be null or empty string.");
        }
        EncryptKeyHelper.createEncryptKey(encryptKeyName.getDb(),
                                            new EncryptKey(encryptKeyName, keyString), ifNotExists);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateEncryptKeyCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
