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

package org.apache.doris.hplsql.executor;

import org.apache.doris.catalog.MysqlColType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.hplsql.exception.QueryException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.StmtExecutor;

import org.antlr.v4.runtime.ParserRuleContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DorisQueryExecutor implements QueryExecutor {
    public DorisQueryExecutor() {
    }

    @Override
    public QueryResult executeQuery(String sql, ParserRuleContext ctx) {
        try {
            // A cursor may correspond to a query, and if the user opens multiple cursors, need to save multiple
            // query states, so here each query constructs a ConnectProcessor and the ConnectContext shares some data.
            ConnectContext context = ConnectContext.get().createContext();
            ConnectProcessor processor = new ConnectProcessor(context);
            processor.executeQuery(sql);
            StmtExecutor executor = context.getExecutor();
            return new QueryResult(new DorisRowResult(executor.getCoord(), executor.getColumns(),
                    executor.getReturnTypes()), () -> metadata(executor), null);
        } catch (Exception e) {
            return new QueryResult(null, () -> new Metadata(Collections.emptyList()), e);
        }
    }

    private Metadata metadata(StmtExecutor stmtExecutor) {
        try {
            List<String> columns = stmtExecutor.getColumns();
            List<Type> types = stmtExecutor.getReturnTypes();
            List<ColumnMeta> colMeta = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                PrimitiveType primitiveType = types.get(i).getPrimitiveType();
                MysqlColType mysqlColType = primitiveType.toMysqlType();
                colMeta.add(new ColumnMeta(columns.get(i), mysqlColType.getTypeName(), primitiveType.toJavaSqlType(),
                        types.get(i)));
            }
            return new Metadata(colMeta);
        } catch (Exception e) {
            throw new QueryException(e);
        }
    }
}
