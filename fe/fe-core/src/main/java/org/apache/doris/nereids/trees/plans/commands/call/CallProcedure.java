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

package org.apache.doris.nereids.trees.plans.commands.call;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.plsql.executor.PlSqlOperation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;

import java.util.Objects;

/**
 * CallProcedure
 */
public class CallProcedure extends CallFunc {
    private final PlSqlOperation executor;
    private final ConnectContext ctx;
    private final String source;

    private CallProcedure(PlSqlOperation executor, ConnectContext ctx, String source) {
        this.executor = Objects.requireNonNull(executor, "executor is missing");
        this.ctx = ctx;
        this.source = source;
    }

    /**
     * Create a CallFunc
     */
    public static CallFunc create(ConnectContext ctx, String source) {
        // The PL/SQL interpreter is wired to the session's MySQL channel: PlSqlOperation.execute()
        // ends with ctx.getMysqlChannel().reset(), PlsqlResult writes rows with
        // MysqlChannel.sendOnePacket(), and PlsqlQueryExecutor runs every inner statement on a
        // ConnectContext.cloneContext() whose connect type is always MYSQL. On an Arrow Flight SQL
        // connection getMysqlChannel() throws, but only after the procedure body has already run,
        // so a CALL carrying DML reports an error to the client while its side effect is applied.
        // Refuse the statement here -- create() runs before run(), so nothing has executed yet.
        if (ctx.getConnectType() != ConnectType.MYSQL) {
            throw new AnalysisException("Stored procedure is only supported on the MySQL protocol,"
                    + " but the current connection type is " + ctx.getConnectType()
                    + ". Please run CALL over a MySQL protocol connection.");
        }
        PlSqlOperation plSqlOperation = ctx.getPlSqlOperation();
        return new CallProcedure(plSqlOperation, ctx, source);
    }

    @Override
    public void run() {
        executor.execute(ctx, source);
    }
}
