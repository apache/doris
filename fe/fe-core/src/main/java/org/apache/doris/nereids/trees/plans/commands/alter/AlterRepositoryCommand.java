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

package org.apache.doris.nereids.trees.plans.commands.alter;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.AlterCommand;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the command for ALTER REPOSITORY for properties.
 */
public class AlterRepositoryCommand extends AlterCommand {
    private final String name;
    private Map<String, String> properties;

    /**
     AlterRepository Constructor
     */
    public AlterRepositoryCommand(String name, Map<String, String> properties) {
        super(PlanType.ALTER_REPOSITORY_COMMAND);
        this.name = name;
        this.properties = properties;
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        FeNameFormat.checkCommonName("repository", name);
        Map<String, String> copyProperties = new HashMap<>(properties);
        if (copyProperties.size() == 0) {
            throw new UserException("alter repository need contains ak/sk/token info of s3.");
        }
        copyProperties.remove(S3Properties.ACCESS_KEY);
        copyProperties.remove(S3Properties.SECRET_KEY);
        copyProperties.remove(S3Properties.SESSION_TOKEN);
        copyProperties.remove(S3Properties.Env.ACCESS_KEY);
        copyProperties.remove(S3Properties.Env.SECRET_KEY);
        copyProperties.remove(S3Properties.Env.TOKEN);
        if (copyProperties.size() != 0) {
            throw new UserException("alter repository only support ak/sk/token info of s3."
                + " unsupported properties: " + copyProperties.keySet());
        }
        Env.getCurrentEnv().getBackupHandler(). alterRepository(name, properties, true);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAlterRepositoryCommand(this, context);
    }

}
