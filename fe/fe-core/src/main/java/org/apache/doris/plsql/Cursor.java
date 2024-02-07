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
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Cursor.java
// and modified by Doris

package org.apache.doris.plsql;

import org.apache.doris.nereids.PLParser.Open_stmtContext;
import org.apache.doris.plsql.executor.QueryExecutor;
import org.apache.doris.plsql.executor.QueryResult;

import org.antlr.v4.runtime.ParserRuleContext;

public class Cursor {
    private String sql;
    private ParserRuleContext sqlExpr;
    private ParserRuleContext sqlSelect;
    private boolean withReturn = false;
    private QueryResult queryResult;

    public enum State {
        OPEN, FETCHED_OK, FETCHED_NODATA, CLOSE
    }

    State state = State.CLOSE;

    public Cursor(String sql) {
        this.sql = sql;
    }

    public void setExprCtx(ParserRuleContext sqlExpr) {
        this.sqlExpr = sqlExpr;
    }

    public void setSelectCtx(ParserRuleContext sqlSelect) {
        this.sqlSelect = sqlSelect;
    }

    public void setWithReturn(boolean withReturn) {
        this.withReturn = withReturn;
    }

    public ParserRuleContext getSqlExpr() {
        return sqlExpr;
    }

    public ParserRuleContext getSqlSelect() {
        return sqlSelect;
    }

    public boolean isWithReturn() {
        return withReturn;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void open(QueryExecutor queryExecutor, Open_stmtContext ctx) {
        this.queryResult = queryExecutor.executeQuery(sql, ctx);
        this.state = State.OPEN;
    }

    public QueryResult getQueryResult() {
        return queryResult;
    }

    /**
     * Set the fetch status
     */
    public void setFetch(boolean ok) {
        if (ok) {
            state = State.FETCHED_OK;
        } else {
            state = State.FETCHED_NODATA;
        }
    }

    public Boolean isFound() {
        if (state == State.OPEN || state == State.CLOSE) {
            return null;
        }
        if (state == State.FETCHED_OK) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    public Boolean isNotFound() {
        if (state == State.OPEN || state == State.CLOSE) {
            return null;
        }
        if (state == State.FETCHED_NODATA) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    public void close() {
        if (queryResult != null) {
            queryResult.close();
            state = State.CLOSE;
        }
    }

    public boolean isOpen() {
        return state != State.CLOSE;
    }
}
