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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import java.util.Objects;

/**
 * MTMV info in alter MTMV.
 */
public abstract class AlterMTMVInfo {
    protected final TableNameInfo mvName;

    /**
     * constructor for alter MTMV
     */
    public AlterMTMVInfo(TableNameInfo mvName) {
        this.mvName = Objects.requireNonNull(mvName, "require mvName object");
    }

    /**
     * analyze alter table info
     */
    public void analyze(ConnectContext ctx) throws AnalysisException {
        mvName.analyze(ctx);
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), mvName.getDb(),
                mvName.getTbl(), PrivPredicate.ALTER)) {
            String message = ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg("ALTER",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    mvName.getDb() + ": " + mvName.getTbl());
            throw new AnalysisException(message);
        }
    }

    public abstract void run() throws UserException;

    public TableNameInfo getMvName() {
        return mvName;
    }

}
