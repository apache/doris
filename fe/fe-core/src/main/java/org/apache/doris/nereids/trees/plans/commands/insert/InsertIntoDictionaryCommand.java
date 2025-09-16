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

/**
 * Command for inserting data into a dictionary. This command is generated from RefreshDictionaryCommand and reuses the
 * logic of InsertIntoTableCommand to maximize code reuse.
 */
public class InsertIntoDictionaryCommand extends InsertIntoTableCommand {
    private final Dictionary dictionary;

    /**
     * Constructor for InsertIntoDictionaryCommand.
     *
     * @param baseCommand The base InsertIntoTableCommand to copy from
     * @param dictionary The target dictionary to insert into
     * @param adaptiveLoad see DictionaryManager.submitDataLoad
     * @throws AnalysisException if the logical query is not a valid sink
     */
    public InsertIntoDictionaryCommand(InsertIntoTableCommand baseCommand, Dictionary dictionary,
            boolean adaptiveLoad) {
        super(baseCommand, PlanType.INSERT_INTO_DICTIONARY_COMMAND);
        this.dictionary = dictionary;

        // Change sink type from olap table(need check) to dictionary
        LogicalPlan logicalQuery = getLogicalQuery();
        if (!(logicalQuery instanceof UnboundTableSink<?>)) {
            throw new AnalysisException("Expected UnboundTableSink but got: " + logicalQuery.getClass().getName());
        }

        UnboundTableSink<?> sink = (UnboundTableSink<?>) logicalQuery;
        UnboundDictionarySink<?> newSink = UnboundTableSinkCreator.createUnboundDictionarySink(dictionary,
                (LogicalPlan) sink.child(0), adaptiveLoad);
        setLogicalQuery(newSink);
        setOriginLogicalQuery(newSink);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        // must run by master
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    protected TableIf getTargetTableIf(ConnectContext ctx, List<String> qualifiedTargetTableName) {
        return dictionary;
    }

    public Dictionary getDictionary() {
        return dictionary;
    }
}
