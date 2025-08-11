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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.StatementContext;

public class ConnectContextUtil {

    // Sometimes it's necessary to parse SQL, but not in a user thread where no ConnectContext exists.
    // In such cases, we need to simulate one—for example,
    // when replaying metadata to parse materialized view (MV) creation statements.
    // Note: After calling this method, ensure to invoke the cleanup() method of ConnectContext.
    public static ConnectContext getDummyCtx(String dbName) {
        ConnectContext ctx = new ConnectContext();
        ctx.setDatabase(dbName);
        StatementContext statementContext = new StatementContext();
        statementContext.setConnectContext(ctx);
        ctx.setStatementContext(statementContext);
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
        ctx.getState().reset();
        ctx.setThreadLocalInfo();
        return ctx;
    }
}
