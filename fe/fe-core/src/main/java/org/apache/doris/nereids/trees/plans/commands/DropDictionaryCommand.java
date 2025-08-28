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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Command for dropping a dictionary.
 */
public class DropDictionaryCommand extends Command implements ForwardWithSync {
    private static final Logger LOG = LogManager.getLogger(DropDictionaryCommand.class);

    private String dbName;

    private final String dictName;

    private final boolean ifExists;

    public DropDictionaryCommand(String dbName, String dictName, boolean ifExists) {
        super(PlanType.DROP_DICTIONARY_COMMAND);
        this.dbName = dbName;
        this.dictName = dictName;
        this.ifExists = ifExists;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DDL;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropDictionaryCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) {
        if (dbName == null) { // use current database
            dbName = ctx.getDatabase();
        }
        try {
            ctx.getEnv().getDictionaryManager().dropDictionary(ctx, dbName, dictName, ifExists);
        } catch (Exception e) {
            String msg = "Failed to drop dictionary " + dictName + " in database " + dbName;
            LOG.warn(msg, e);
            throw new AnalysisException(msg + ": " + e.getMessage());
        }
    }
}
