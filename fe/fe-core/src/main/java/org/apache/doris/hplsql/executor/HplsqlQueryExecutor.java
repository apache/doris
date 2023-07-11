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

package org.apache.doris.hplsql.executor;

import org.apache.doris.common.ErrorCode;
import org.apache.doris.hplsql.Arguments;
import org.apache.doris.hplsql.Conf;
import org.apache.doris.hplsql.Exec;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HplsqlQueryExecutor {
    private static final Logger LOG = LogManager.getLogger(HplsqlQueryExecutor.class);

    private HplsqlResult result;

    private Exec exec;

    public HplsqlQueryExecutor(ConnectProcessor processor) {
        result = new HplsqlResult(processor);
        exec = new Exec(new Conf(), result, new DorisQueryExecutor(), result);
        exec.init();
    }

    public void execute(String statement) {
        ConnectContext context = ConnectContext.get();
        result.reset();
        try {
            Arguments args = new Arguments();
            args.parse(new String[] {"-e", statement});
            exec.parseAndEval(args);

            exec.printExceptions();
            String error = result.getError();
            String msg = result.getMsg();
            if (!error.isEmpty()) {
                context.getState().setError("hplsql exec error, " + error);
            } else if (!msg.isEmpty()) {
                context.getState().setOk(0, 0, msg);
            }
        } catch (Exception e) {
            exec.printExceptions();
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, result.getError() + " " + e.getMessage());
            LOG.warn(e);
        }
    }
}
