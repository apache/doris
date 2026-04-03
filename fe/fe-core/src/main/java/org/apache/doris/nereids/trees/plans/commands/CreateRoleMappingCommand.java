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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * CREATE ROLE MAPPING command entry.
 */
public class CreateRoleMappingCommand extends Command implements ForwardWithSync {
    private final String mappingName;
    private final boolean ifNotExists;
    private final String integrationName;
    private final List<RoleMappingRule> rules;
    private final String comment;

    /**
     * Create a role mapping command.
     */
    public CreateRoleMappingCommand(String mappingName, boolean ifNotExists,
            String integrationName, List<RoleMappingRule> rules, String comment) {
        super(PlanType.CREATE_ROLE_MAPPING_COMMAND);
        this.mappingName = Objects.requireNonNull(mappingName, "mappingName can not be null");
        this.ifNotExists = ifNotExists;
        this.integrationName = Objects.requireNonNull(integrationName, "integrationName can not be null");
        this.rules = Collections.unmodifiableList(new ArrayList<>(
                Objects.requireNonNull(rules, "rules can not be null")));
        this.comment = comment;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateRoleMappingCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        Env.getCurrentEnv().getRoleMappingMgr().createRoleMapping(mappingName, ifNotExists,
                integrationName, rules, comment,
                Objects.requireNonNull(ctx.getQualifiedUser(), "qualifiedUser can not be null"));
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }

    public String getMappingName() {
        return mappingName;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public String getIntegrationName() {
        return integrationName;
    }

    public List<RoleMappingRule> getRules() {
        return rules;
    }

    public String getComment() {
        return comment;
    }

    /**
     * Immutable CREATE ROLE MAPPING rule payload.
     */
    public static final class RoleMappingRule {
        private final String condition;
        private final Set<String> grantedRoles;

        public RoleMappingRule(String condition, Set<String> grantedRoles) {
            this.condition = Objects.requireNonNull(condition, "condition can not be null");
            this.grantedRoles = Collections.unmodifiableSet(new LinkedHashSet<>(
                    Objects.requireNonNull(grantedRoles, "grantedRoles can not be null")));
        }

        public String getCondition() {
            return condition;
        }

        public Set<String> getGrantedRoles() {
            return grantedRoles;
        }
    }
}
