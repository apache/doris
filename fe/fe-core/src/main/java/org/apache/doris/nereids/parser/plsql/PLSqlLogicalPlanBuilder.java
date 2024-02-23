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

package org.apache.doris.nereids.parser.plsql;

import org.apache.doris.nereids.DorisParser.ColumnReferenceContext;
import org.apache.doris.nereids.PLParser.MultipartIdentifierContext;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.LogicalPlanBuilder;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.plsql.Var;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.RuleContext;

import java.util.List;

/**
 * Extends from {@link org.apache.doris.nereids.parser.LogicalPlanBuilder},
 * just focus on the difference between these query syntax.
 */
public class PLSqlLogicalPlanBuilder extends LogicalPlanBuilder {

    public List<String> visitMultipartIdentifier(MultipartIdentifierContext ctx) {
        return ctx.parts.stream()
                .map(RuleContext::getText)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public Expression visitColumnReference(ColumnReferenceContext ctx) {
        Var var = ConnectContext.get().getProcedureExec().findVariable(ctx.getText());
        if (var != null) {
            return var.toLiteral();
        }
        return UnboundSlot.quoted(ctx.getText());
    }
}
