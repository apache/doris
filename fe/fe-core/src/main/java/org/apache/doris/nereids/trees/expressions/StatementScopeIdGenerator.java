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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.TableId;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;

/**
 * The util of named expression.
 */
public class StatementScopeIdGenerator {

    // for ut test only, ExprId starts with 10000 to avoid duplicate ExprId. In ut, before creating ConnectContext,
    // table is already created, and hence column.exprId may be recreated during applying rules.
    private static StatementContext statementContext = new StatementContext(10000);

    public static ExprId newExprId() {
        // this branch is for test only
        if (ConnectContext.get() == null || ConnectContext.get().getStatementContext() == null) {
            return statementContext.getNextExprId();
        }
        return ConnectContext.get().getStatementContext().getNextExprId();
    }

    public static ObjectId newObjectId() {
        // this branch is for test only
        if (ConnectContext.get() == null || ConnectContext.get().getStatementContext() == null) {
            return statementContext.getNextObjectId();
        }
        return ConnectContext.get().getStatementContext().getNextObjectId();
    }

    public static RelationId newRelationId() {
        // this branch is for test only
        if (ConnectContext.get() == null || ConnectContext.get().getStatementContext() == null) {
            return statementContext.getNextRelationId();
        }
        return ConnectContext.get().getStatementContext().getNextRelationId();
    }

    public static CTEId newCTEId() {
        // this branch is for test only
        if (ConnectContext.get() == null || ConnectContext.get().getStatementContext() == null) {
            return statementContext.getNextCTEId();
        }
        return ConnectContext.get().getStatementContext().getNextCTEId();
    }

    public static TableId newTableId() {
        if (ConnectContext.get() == null || ConnectContext.get().getStatementContext() == null) {
            return statementContext.getNextTableId();
        }
        return ConnectContext.get().getStatementContext().getNextTableId();
    }

    /**
     *  Reset Id Generator
     */
    @VisibleForTesting
    public static void clear() throws Exception {
        if (ConnectContext.get() != null) {
            ConnectContext.get().setStatementContext(new StatementContext());
        }
        statementContext = new StatementContext();
    }
}
