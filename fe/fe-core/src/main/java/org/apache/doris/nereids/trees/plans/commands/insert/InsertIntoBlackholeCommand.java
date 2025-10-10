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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.nereids.analyzer.UnboundDictionarySink;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import java.util.List;
import java.util.Optional;

/**
 *
 */
public class InsertIntoBlackholeCommand extends InsertIntoTableCommand {
    // LogicalPlan logicalQuery, Optional<String> labelName,
    //             Optional<InsertCommandContext> insertCtx, Optional<LogicalPlan> cte
    public InsertIntoBlackholeCommand(LogicalPlan logicalQuery) {
        super(PlanType.INSERT_INTO_BLACKHOLE_COMMAND, logicalQuery, Optional.empty(), Optional.empty(),
                Optional.empty(), true, Optional.empty());
    }

    // @Override
    // public RedirectStatus toRedirectStatus() {
    //     // must run by master
    //     return RedirectStatus.FORWARD_WITH_SYNC;
    // }

}
