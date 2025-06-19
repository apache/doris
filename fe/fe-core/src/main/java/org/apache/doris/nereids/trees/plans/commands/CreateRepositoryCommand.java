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
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Map;

/**
 * CreateRepositoryCommand
 */
public class CreateRepositoryCommand extends Command implements ForwardWithSync, NeedAuditEncryption {
    private boolean isReadOnly;
    private String name;
    private StorageBackend storage;

    public CreateRepositoryCommand(boolean isReadOnly, String name, StorageBackend storage) {
        super(PlanType.CREATE_REPOSITORY_COMMAND);
        this.isReadOnly = isReadOnly;
        this.name = name;
        this.storage = storage;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        Env.getCurrentEnv().getBackupHandler().createRepository(this);
    }

    /**
     * validate
     */
    public void validate() throws UserException {
        storage.validate();
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        FeNameFormat.checkRepositoryName(name);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateRepositoryCommand(this, context);
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public String getName() {
        return name;
    }

    public String getBrokerName() {
        return storage.getStorageDesc().getName();
    }

    public StorageBackend.StorageType getStorageType() {
        return storage.getStorageDesc().getStorageType();
    }

    public String getLocation() {
        return storage.getLocation();
    }

    public Map<String, String> getProperties() {
        return storage.getStorageDesc().getProperties();
    }

    public StorageProperties getStorageProperties() {
        return storage.getStorageDesc().getStorageProperties();
    }
}
