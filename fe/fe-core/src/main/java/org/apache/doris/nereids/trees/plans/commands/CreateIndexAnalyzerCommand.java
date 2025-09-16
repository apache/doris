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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.indexpolicy.IndexPolicyTypeEnum;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Map;

/**
 * CREATE INVERTED INDEX ANALYZER [IF NOT EXISTS] policy_name PROPERTIES (key1 = value1, ...)
 */
public class CreateIndexAnalyzerCommand extends Command implements ForwardWithSync {

    private final boolean ifNotExists;
    private final String policyName;
    private final Map<String, String> properties;

    /**
     * Constructor
     */
    public CreateIndexAnalyzerCommand(boolean ifNotExists, String policyName, Map<String, String> properties) {
        super(PlanType.CREATE_INDEX_ANALYZER_COMMAND);
        this.ifNotExists = ifNotExists;
        this.policyName = policyName;
        this.properties = properties;
    }

    private void validate(ConnectContext ctx) throws UserException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // check name
        FeNameFormat.checkIndexPolicyName(policyName);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws UserException {
        validate(ctx);

        Env.getCurrentEnv().getIndexPolicyMgr().createIndexPolicy(ifNotExists, policyName,
                IndexPolicyTypeEnum.ANALYZER, properties);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateIndexAnalyzerCommand(this, context);
    }
}
