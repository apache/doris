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

package org.apache.doris.nereids;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.rules.analysis.CTEContext;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

/**
 * Statement context for nereids
 */
public class StatementContext {

    private ConnectContext connectContext;

    private OriginStatement originStatement;

    private final IdGenerator<ExprId> exprIdGenerator = ExprId.createGenerator();

    private final IdGenerator<RelationId> relationIdGenerator = RelationId.createGenerator();

    private StatementBase parsedStatement;

    private CTEContext cteContext;

    public StatementContext() {
    }

    public StatementContext(ConnectContext connectContext, OriginStatement originStatement) {
        this(connectContext, originStatement, new CTEContext());
    }

    public StatementContext(ConnectContext connectContext, OriginStatement originStatement, CTEContext cteContext) {
        this.connectContext = connectContext;
        this.originStatement = originStatement;
        this.cteContext = cteContext;
    }

    public void setConnectContext(ConnectContext connectContext) {
        this.connectContext = connectContext;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public void setOriginStatement(OriginStatement originStatement) {
        this.originStatement = originStatement;
    }

    public OriginStatement getOriginStatement() {
        return originStatement;
    }

    public StatementBase getParsedStatement() {
        return parsedStatement;
    }

    public ExprId getNextExprId() {
        return exprIdGenerator.getNextId();
    }

    public RelationId getNextRelationId() {
        return relationIdGenerator.getNextId();
    }

    public CTEContext getCteContext() {
        return cteContext;
    }

    public void setCteContext(CTEContext cteContext) {
        this.cteContext = cteContext;
    }

    public void setParsedStatement(StatementBase parsedStatement) {
        this.parsedStatement = parsedStatement;
    }
}
