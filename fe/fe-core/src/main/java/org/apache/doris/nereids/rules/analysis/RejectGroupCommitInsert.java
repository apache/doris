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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * reject group commit insert added by PR <a href="https://github.com/apache/doris/pull/22829/files">#22829</a>
 */
public class RejectGroupCommitInsert implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalOlapTableSink(logicalOneRowRelation())
                        .thenApply(ctx -> {
                            if (ctx.connectContext.getSessionVariable().enableInsertGroupCommit) {
                                throw new AnalysisException("Nereids do not support group commit now.");
                            }
                            return null;
                        }).toRule(RuleType.REJECT_GROUP_COMMIT_INSERT),
                logicalOlapTableSink(logicalUnion().when(u -> u.arity() == 0))
                        .thenApply(ctx -> {
                            if (ctx.connectContext.getSessionVariable().enableInsertGroupCommit) {
                                throw new AnalysisException("Nereids do not support group commit now.");
                            }
                            return null;
                        }).toRule(RuleType.REJECT_GROUP_COMMIT_INSERT)
        );
    }
}
