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

package org.apache.doris.analysis;

import org.apache.doris.common.UserException;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Drop stage statement.
 * syntax:
 * DROP STAGE [IF EXISTS] stage_name
 **/
@AllArgsConstructor
public class DropStageStmt extends DdlStmt {

    @Getter
    private final boolean ifExists;

    @Getter
    private final String stageName;

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        // todo(copy into): check drop stage permission
        // if (!Env.getCurrentEnv().getAuth()
        //         .checkCloudPriv(ConnectContext.get().getCurrentUserIdentity(), stageName, PrivPredicate.ADMIN,
        //                 ResourceTypeEnum.STAGE)) {
        //     throw new AnalysisException(
        //             "ADMIN denied to user '" + ConnectContext.get().getQualifiedUser() + "'@'" + ConnectContext.get()
        //                     .getRemoteIP() + "' for cloud stage '" + stageName + "'");
        // }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP ").append(" STAGE ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(stageName);
        return sb.toString();
    }
}
