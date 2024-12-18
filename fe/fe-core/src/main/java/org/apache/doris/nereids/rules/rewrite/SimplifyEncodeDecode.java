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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DecodeAsVarchar;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncodeStrToInteger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * project (..., encode(decode(A)) as B, ...)
 * =>
 * project (..., A as B,...)
 */
public class SimplifyEncodeDecode implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.SIMPLIFY_ENCODE_DECODE.build(
                        logicalProject()
                                .then(project -> {
                                    List<NamedExpression> newProjections =
                                            Lists.newArrayListWithCapacity(project.getProjects().size());
                                    boolean changed = false;
                                    for (NamedExpression namedExpression : project.getProjects()) {
                                        if (namedExpression instanceof Alias
                                                && namedExpression.child(0) instanceof EncodeStrToInteger
                                                && namedExpression.child(0).child(0)
                                                instanceof DecodeAsVarchar) {
                                            Alias alias = (Alias) namedExpression;
                                            Expression body = namedExpression.child(0)
                                                    .child(0).child(0);
                                            newProjections.add((Alias) alias.withChildren(body));
                                            changed = true;
                                        } else {
                                            newProjections.add(namedExpression);
                                        }
                                    }
                                    return changed ? project.withProjects(newProjections) : project;
                                })
                )
        );
    }
}
