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

import org.apache.doris.nereids.StatementContext;

public class AutoCloseConnectContext implements AutoCloseable {

    public final ConnectContext connectContext;

    private final ConnectContext previousContext;

    public AutoCloseConnectContext(ConnectContext connectContext) {
        this.previousContext = ConnectContext.get();
        this.connectContext = connectContext;
        connectContext.setThreadLocalInfo();
    }

    public void call() {
        // try (AutoCloseConnectContext autoCloseCtx = new AutoCloseConnectContext(context)) {
        // will report autoCloseCtx is not used, so call an empty method.
    }

    @Override
    public void close() {
        // The statement this context ran (an EXPORT's SELECT INTO OUTFILE, an ANALYZE's statistics
        // query, ...) is over with the block: end it the way ConnectProcessor ends a connection's
        // statement, before clear() drops the reference. What a plan opened for a coordinator that
        // never came - the Flight SQL session of a remote Doris scan when the statement failed
        // between planning and dispatch - is released here. Idempotent, and the planner has
        // released its table locks at the end of planning already.
        StatementContext statementContext = connectContext.getStatementContext();
        if (statementContext != null) {
            statementContext.close();
        }
        connectContext.clear();
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }
}
