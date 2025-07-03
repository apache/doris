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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.EncryptKeySearchDesc;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * drop encrypt key command
 */
public class DropEncryptkeyCommand extends DropCommand {
    private final boolean ifExists;
    private final EncryptKeyName encryptKeyName;

    /**
     * constructor
     */
    public DropEncryptkeyCommand(EncryptKeyName encryptKeyName, boolean ifExists) {
        super(PlanType.DROP_ENCRYPTKEY_COMMAND);
        this.encryptKeyName = encryptKeyName;
        this.ifExists = ifExists;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check operation privilege
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        // analyze encryptkey name
        encryptKeyName.analyze(ctx);
        EncryptKeySearchDesc encryptKeySearchDesc = new EncryptKeySearchDesc(encryptKeyName);
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(encryptKeyName.getDb());
        db.dropEncryptKey(encryptKeySearchDesc, ifExists);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropEncryptKeyCommand(this, context);
    }
}
