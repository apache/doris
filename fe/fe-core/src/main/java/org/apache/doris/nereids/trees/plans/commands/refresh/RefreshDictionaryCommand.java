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

package org.apache.doris.nereids.trees.plans.commands.refresh;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.dictionary.DictionaryManager;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * Refresh table command.
 */
public class RefreshDictionaryCommand extends Command implements ForwardWithSync {
    private String dbName;

    private String dictionaryName;

    public RefreshDictionaryCommand(String dbName, String dicName) {
        super(PlanType.REFRESH_DICTIONARY_COMMAND);
        this.dbName = dbName;
        this.dictionaryName = dicName;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        DictionaryManager dictionaryManager = ctx.getEnv().getDictionaryManager();
        String db = dbName == null ? ctx.getDatabase() : dbName;
        Dictionary dictionary = dictionaryManager.getDictionary(db, dictionaryName);
        dictionaryManager.dataLoad(ctx, dictionary, false);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitRefreshDictionaryCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.REFRESH;
    }
}
