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

package org.apache.doris.plsql.executor;

import org.apache.doris.common.ErrorCode;
import org.apache.doris.plsql.Arguments;
import org.apache.doris.plsql.Conf;
import org.apache.doris.plsql.Exec;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PlSqlOperation {
    private static final Logger LOG = LogManager.getLogger(PlSqlOperation.class);

    private final PlsqlResult result;

    private final Exec exec;

    public PlSqlOperation() {
        result = new PlsqlResult();
        exec = new Exec(new Conf(), result, new PlsqlQueryExecutor(), result);
        exec.init();
    }

    public Exec getExec() {
        return exec;
    }

    public void execute(ConnectContext ctx, String statement) {
        try {
            ctx.setRunProcedure(true);
            ctx.setProcedureExec(exec);
            result.reset();
            try {
                Arguments args = new Arguments();
                args.parse(new String[] {"-e", statement});
                exec.parseAndEval(args);
            } catch (Exception e) {
                exec.signal(e);
            } finally {
                // Exception is not thrown after catch.
                // For example, select a not exist table will return empty results, exception
                // will put into signals.
                exec.printExceptions();
                String error = result.getError();
                String msg = result.getMsg();
                if (!error.isEmpty()) {
                    ctx.getState().setError(result.getLastErrorCode(), error);
                } else if (!msg.isEmpty()) {
                    ctx.getState().setOk(0, 0, msg);
                }
                ctx.getMysqlChannel().reset();
                ctx.setRunProcedure(false);
                ctx.setProcedureExec(null);
            }
        } catch (Exception e) {
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, e.getMessage());
            LOG.warn(e);
        }
    }
}
