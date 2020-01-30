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

package org.apache.doris.utframe;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import java.io.StringReader;

public class UtFrameUtils {

    // Help to create a mocked ConnectContext.
    public static ConnectContext createDefaultCtx() {
        ConnectContext ctx = new ConnectContext();
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setQualifiedUser(PaloAuth.ROOT_USER);
        ctx.setCatalog(Catalog.getCurrentCatalog());
        ctx.setThreadLocalInfo();
        return ctx;
    }

    // Parse an origin stmt and analyze it. Return a StatementBase instance.
    public static StatementBase parseAndAnalyzeStmt(String originStmt, ConnectContext ctx)
            throws Exception {
        System.out.println("begin to parse stmt: " + originStmt);
        SqlScanner input = new SqlScanner(new StringReader(originStmt), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);

        StatementBase statementBase = (StatementBase) parser.parse().value;
        Analyzer analyzer = new Analyzer(ctx.getCatalog(), ctx);
        statementBase.analyze(analyzer);
        return statementBase;
    }
}
