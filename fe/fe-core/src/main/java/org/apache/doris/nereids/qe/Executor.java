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

package org.apache.doris.nereids.qe;

import org.apache.doris.nereids.parser.SqlParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Temporary executor in Nereids.
 */
public class Executor {
    private static final Logger LOG = LogManager.getLogger(Executor.class);

    private final String sql;
    private final ConnectContext context;

    public Executor(String sql, ConnectContext context) {
        this.sql = sql;
        this.context = context;
    }

    public void dryRun() throws Exception {
        doExecute(false);
    }

    public void execute() throws Exception {
        doExecute(true);
    }

    private void doExecute(boolean sendFragments) throws Exception {
        LOG.info("==== input SQL: ====\n{}", sql);
        System.out.println("==== input SQL: ====\n" + sql + "\n");

        // parse phase
        SqlParser parser = new SqlParser();
        LogicalPlan parsedPlan = parser.parse(sql);
        LOG.info("==== parsed plan: ====\n{}", parsedPlan.treeString());
        System.out.println("==== parsed plan: ====\n" + parsedPlan.treeString() + "\n");
    }
}
