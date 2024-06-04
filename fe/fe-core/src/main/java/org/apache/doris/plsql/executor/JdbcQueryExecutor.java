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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/executor/JdbcQueryExecutor.java
// and modified by Doris

package org.apache.doris.plsql.executor;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.plsql.Exec;
import org.apache.doris.plsql.Query;
import org.apache.doris.plsql.exception.QueryException;

import org.antlr.v4.runtime.ParserRuleContext;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JdbcQueryExecutor implements QueryExecutor {
    private final Exec exec;

    public JdbcQueryExecutor(Exec exec) {
        this.exec = exec;
    }

    @Override
    public QueryResult executeQuery(String sql, ParserRuleContext ctx) {
        String conn = exec.getStatementConnection();
        Query query = exec.executeQuery(ctx, new Query(sql), conn);
        ResultSet resultSet = query.getResultSet();
        if (resultSet == null) { // offline mode
            return new QueryResult(null, () -> new Metadata(Collections.emptyList()), null, query.getException());
        } else {
            return new QueryResult(new JdbcRowResult(resultSet), () -> metadata(resultSet), null, query.getException());
        }
    }

    private static Metadata metadata(ResultSet resultSet) {
        try {
            ResultSetMetaData meta = resultSet.getMetaData();
            List<ColumnMeta> colMetas = new ArrayList<>();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                colMetas.add(new ColumnMeta(meta.getColumnName(i), meta.getColumnTypeName(i), meta.getColumnType(i)));
            }
            return new Metadata(colMetas);
        } catch (SQLException e) {
            throw new QueryException(e);
        }
    }

    private static class JdbcRowResult implements org.apache.doris.plsql.executor.RowResult {
        private final ResultSet resultSet;

        private JdbcRowResult(ResultSet resultSet) {
            this.resultSet = resultSet;
        }

        @Override
        public boolean next() {
            try {
                return resultSet.next();
            } catch (SQLException e) {
                throw new QueryException(e);
            }
        }

        @Override
        public <T> T get(int columnIndex, Class<T> type) {
            try {
                return (T) resultSet.getObject(columnIndex + 1);
            } catch (SQLException e) {
                throw new QueryException(e);
            }
        }

        @Override
        public Literal get(int columnIndex) throws AnalysisException {
            throw new RuntimeException("no support get Doris type result");
        }

        @Override
        public ByteBuffer getMysqlRow() {
            throw new RuntimeException("not implement getMysqlRow method.");
        }

        @Override
        public void close() {
            try {
                resultSet.close();
            } catch (SQLException e) {
                throw new QueryException(e);
            }
        }
    }
}
