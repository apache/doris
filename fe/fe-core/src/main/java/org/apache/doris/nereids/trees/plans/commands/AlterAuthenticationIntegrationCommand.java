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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * ALTER AUTHENTICATION INTEGRATION command entry.
 */
public class AlterAuthenticationIntegrationCommand extends AlterCommand implements NeedAuditEncryption {
    /** alter action. */
    public enum AlterType {
        SET_PROPERTIES,
        SET_COMMENT
    }

    private final String integrationName;
    private final AlterType alterType;
    private final Map<String, String> properties;
    private final String comment;

    private AlterAuthenticationIntegrationCommand(String integrationName, AlterType alterType,
            Map<String, String> properties, String comment) {
        super(PlanType.ALTER_AUTHENTICATION_INTEGRATION_COMMAND);
        this.integrationName = Objects.requireNonNull(integrationName, "integrationName can not be null");
        this.alterType = Objects.requireNonNull(alterType, "alterType can not be null");
        this.properties = Collections.unmodifiableMap(
                new LinkedHashMap<>(Objects.requireNonNull(properties, "properties can not be null")));
        this.comment = comment;
    }

    public static AlterAuthenticationIntegrationCommand forSetProperties(String integrationName,
            Map<String, String> properties) {
        return new AlterAuthenticationIntegrationCommand(
                integrationName, AlterType.SET_PROPERTIES, properties, null);
    }

    public static AlterAuthenticationIntegrationCommand forSetComment(String integrationName, String comment) {
        return new AlterAuthenticationIntegrationCommand(
                integrationName, AlterType.SET_COMMENT, Collections.emptyMap(), comment);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        switch (alterType) {
            case SET_PROPERTIES:
                Env.getCurrentEnv().getAuthenticationIntegrationMgr()
                        .alterAuthenticationIntegrationProperties(integrationName, properties);
                return;
            case SET_COMMENT:
                Env.getCurrentEnv().getAuthenticationIntegrationMgr()
                        .alterAuthenticationIntegrationComment(integrationName, comment);
                return;
            default:
                throw new AnalysisException("Unsupported alter type for AUTHENTICATION INTEGRATION: " + alterType);
        }
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    public String getIntegrationName() {
        return integrationName;
    }

    public AlterType getAlterType() {
        return alterType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getComment() {
        return comment;
    }
}
