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
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncodeString;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * in project:
 * decode_as_varchar(encode_as_xxx(v)) => v
 */
public class DecoupleEncodeDecode extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalProject()
                .when(topN -> ConnectContext.get() != null
                        && ConnectContext.get().getSessionVariable().enableCompressMaterialize)
                .then(this::rewrite)
                .toRule(RuleType.DECOUPLE_DECODE_ENCODE_SLOT);
    }

    private LogicalProject<?> rewrite(LogicalProject<?> project) {
        List<NamedExpression> newProjections = Lists.newArrayList();
        boolean hasNewProjections = false;
        for (NamedExpression e : project.getProjects()) {
            boolean changed = false;
            if (e instanceof Alias) {
                Alias alias = (Alias) e;
                Expression body = alias.child();
                if (body instanceof DecodeAsVarchar && body.child(0) instanceof EncodeString) {
                    Expression encodeBody = body.child(0).child(0);
                    newProjections.add((NamedExpression) alias.withChildren(encodeBody));
                    changed = true;
                } else if (body instanceof EncodeString && body.child(0) instanceof DecodeAsVarchar) {
                    Expression decodeBody = body.child(0).child(0);
                    newProjections.add((NamedExpression) alias.withChildren(decodeBody));
                    changed = true;
                }
            }
            if (!changed) {
                newProjections.add(e);
                hasNewProjections = true;
            }
        }
        if (hasNewProjections) {
            project = project.withProjects(newProjections);
        }
        return project;
    }

}
