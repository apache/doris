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

import org.apache.doris.analysis.DbName;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * CreateDatabaseCommand
 */
public class CreateDatabaseCommand extends Command implements ForwardWithSync, NeedAuditEncryption {
    private boolean ifNotExists;
    private String ctlName;
    private String dbName;
    private Map<String, String> properties;

    /**
     * CreateDatabaseCommand
     */
    public CreateDatabaseCommand(boolean ifNotExists, DbName dbName, Map<String, String> properties) {
        super(PlanType.CREATE_DATABASE_COMMAND);
        this.ifNotExists = ifNotExists;
        this.ctlName = dbName.getCtl();
        this.dbName = dbName.getDb();
        this.properties = properties == null ? new HashMap<>() : properties;

        if (Config.force_enable_feature_binlog
                && !this.properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE)) {
            this.properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE, "true");
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        Env.getCurrentEnv().createDb(this);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws UserException {
        if (StringUtils.isEmpty(ctlName)) {
            ctlName = Env.getCurrentEnv().getCurrentCatalog().getName();
        }
        FeNameFormat.checkCatalogName(ctlName);
        FeNameFormat.checkDbName(dbName);
        InternalDatabaseUtil.checkDatabase(dbName, ConnectContext.get());
        if (!Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(), ctlName, dbName, PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_DBACCESS_DENIED_ERROR, ctx.getQualifiedUser(), dbName);
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateDatabaseCommand(this, context);
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getCtlName() {
        return ctlName;
    }

    public String getDbName() {
        return dbName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
