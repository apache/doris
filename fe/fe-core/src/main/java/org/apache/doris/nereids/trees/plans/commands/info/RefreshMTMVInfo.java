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
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import java.util.Objects;

/**
 * refresh mtmv info
 */
public class RefreshMTMVInfo {
    private final TableNameInfo mvName;

    public RefreshMTMVInfo(TableNameInfo mvName) {
        this.mvName = Objects.requireNonNull(mvName, "require mvName object");
    }

    /**
     * analyze refresh info
     * @param ctx ConnectContext
     */
    public void analyze(ConnectContext ctx) {
        mvName.analyze(ctx);
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), mvName.getDb(),
                mvName.getTbl(), PrivPredicate.CREATE)) {
            throw new AnalysisException("Access denied");
        }
    }

    /**
     * getMvName
     * @return TableNameInfo
     */
    public TableNameInfo getMvName() {
        return mvName;
    }
}
