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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.common.ErrorCode;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.qe.ConnectContext;

/**
 * Check whether a user is permitted to scan specific tables.
 */
public class UserAuthentication extends OneAnalysisRuleFactory {

    @Override
    public Rule build() {
        return logicalRelation().thenApply(ctx -> checkPermission(ctx.root, ctx.connectContext))
                .toRule(RuleType.RELATION_AUTHENTICATION);
    }

    private Plan checkPermission(LogicalRelation relation, ConnectContext connectContext) {
        String dbName = !relation.getQualifier().isEmpty() ? relation.getQualifier().get(0) : null;
        String tableName = relation.getTable().getName();
        if (!connectContext.getEnv().getAuth().checkTblPriv(connectContext, dbName, tableName, PrivPredicate.SELECT)) {
            String message = ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg("SELECT",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    dbName + ": " + tableName);
            throw new AnalysisException(message);

        }
        return relation;
    }
}
