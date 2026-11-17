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
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Util;
import org.apache.doris.nereids.StatementContext;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

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

    public static Optional<Pair<ErrorCode, String>> initCatalogAndDb(ConnectContext ctx, String fullDbName) {
        String catalogName = null;
        String dbName = null;
        String[] dbNames = fullDbName.split("\\.");
        if (dbNames.length == 1) {
            dbName = fullDbName;
        } else if (dbNames.length == 2) {
            catalogName = dbNames[0];
            dbName = dbNames[1];
        } else if (dbNames.length > 2) {
            if (GlobalVariable.enableNestedNamespace) {
                // use the first part as catalog name, the rest part as db name
                catalogName = dbNames[0];
                dbName = Stream.of(dbNames).skip(1).reduce((a, b) -> a + "." + b).get();
            } else {
                return Optional.of(
                        Pair.of(ErrorCode.ERR_BAD_DB_ERROR, "Only one dot can be in the name: " + fullDbName));
            }
        }

        //  mysql client
        if (Config.isCloudMode()) {
            try {
                dbName = ((CloudEnv) ctx.getEnv()).analyzeCloudCluster(dbName, ctx);
            } catch (DdlException e) {
                return Optional.of(Pair.of(e.getMysqlErrorCode(), e.getMessage()));
            }
            if (dbName == null || dbName.isEmpty()) {
                return Optional.empty();
            }
        }

        try {
            if (catalogName != null) {
                ctx.getEnv().changeCatalog(ctx, catalogName);
            }
            ctx.getEnv().changeDb(ctx, dbName);
        } catch (DdlException e) {
            return Optional.of(Pair.of(e.getMysqlErrorCode(), e.getMessage()));
        } catch (Throwable t) {
            return Optional.of(Pair.of(ErrorCode.ERR_INTERNAL_ERROR, Util.getRootCauseMessage(t)));
        }
        ctx.getState().setOk();
        return Optional.empty();
    }

    public static Map<String, String> getAffectQueryResultInPlanVariables(ConnectContext ctx) {
        if (ctx == null || ctx.getSessionVariable() == null) {
            return null;
        }
        return ctx.getSessionVariable().getAffectQueryResultInPlanVariables();
    }
}
