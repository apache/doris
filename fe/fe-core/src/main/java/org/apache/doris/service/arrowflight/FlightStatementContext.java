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

package org.apache.doris.service.arrowflight;

import org.apache.doris.analysis.Expr;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.util.AutoCloseables;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Objects;

/**
 * Context for {@link T} to be persisted in memory in between {@link FlightSqlProducer} calls.
 *
 * @param <T> the {@link Statement} to be persisted.
 */
public final class FlightStatementContext<T extends Statement> implements AutoCloseable {

    private final T statement;
    private final String query;

    private TUniqueId queryId;

    private TUniqueId finstId;

    private TNetworkAddress resultFlightServerAddr;

    private TNetworkAddress resultInternalServiceAddr;

    private ArrayList<Expr> resultOutputExprs;

    public FlightStatementContext(final T statement, final String query) {
        this.statement = Objects.requireNonNull(statement, "statement cannot be null.");
        this.query = query;
    }

    public void setQueryId(TUniqueId queryId) {
        this.queryId = queryId;
    }

    public void setFinstId(TUniqueId finstId) {
        this.finstId = finstId;
    }

    public void setResultFlightServerAddr(TNetworkAddress resultFlightServerAddr) {
        this.resultFlightServerAddr = resultFlightServerAddr;
    }

    public void setResultInternalServiceAddr(TNetworkAddress resultInternalServiceAddr) {
        this.resultInternalServiceAddr = resultInternalServiceAddr;
    }

    public void setResultOutputExprs(ArrayList<Expr> resultOutputExprs) {
        this.resultOutputExprs = resultOutputExprs;
    }

    public T getStatement() {
        return statement;
    }

    public String getQuery() {
        return query;
    }

    public TUniqueId getQueryId() {
        return queryId;
    }

    public TUniqueId getFinstId() {
        return finstId;
    }

    public TNetworkAddress getResultFlightServerAddr() {
        return resultFlightServerAddr;
    }

    public TNetworkAddress getResultInternalServiceAddr() {
        return resultInternalServiceAddr;
    }

    public ArrayList<Expr> getResultOutputExprs() {
        return resultOutputExprs;
    }

    @Override
    public void close() throws Exception {
        Connection connection = statement.getConnection();
        AutoCloseables.close(statement, connection);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FlightStatementContext)) {
            return false;
        }
        final FlightStatementContext<?> that = (FlightStatementContext<?>) other;
        return statement.equals(that.statement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statement);
    }
}
