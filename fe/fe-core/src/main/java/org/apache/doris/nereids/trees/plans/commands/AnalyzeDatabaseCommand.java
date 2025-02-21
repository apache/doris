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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.AnalyzeProperties;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * AnalyzeDatabaseCommand
 */
public class AnalyzeDatabaseCommand extends AnalyzeCommand {
    private final String ctlName;
    private final String dbName;

    private CatalogIf ctlIf;

    private DatabaseIf<TableIf> db;

    /**
     * AnalyzeCommand
     */
    public AnalyzeDatabaseCommand(String ctlName, String dbName, AnalyzeProperties properties) {
        super(PlanType.ANALYZE_DATABASE, properties);
        this.ctlName = ctlName;
        this.dbName = dbName;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        super.validate(ctx);
        if (ctlName == null) {
            ctlIf = Env.getCurrentEnv().getCurrentCatalog();
        } else {
            ctlIf = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(ctlName);
        }
        db = ctlIf.getDbOrAnalysisException(dbName);
    }

    public DatabaseIf<TableIf> getDb() {
        return db;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ctx.getEnv().analyze(this, executor.isProxy());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

}
