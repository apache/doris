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

package org.apache.doris.analysis;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;

/**
 * Visitor that produces the external SQL string for any {@link Expr}.
 * Extends {@link ExprToSqlVisitor} and overrides only the methods that differ
 * for external SQL generation.
 *
 * <p>Usage examples:
 * <pre>
 *   // equivalent to expr.toSqlWithoutTbl()
 *   expr.accept(ExprToExternalSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE)
 *
 *   // equivalent to expr.toExternalSql(tableType, table)
 *   expr.accept(ExprToExternalSqlVisitor.INSTANCE, new ToSqlParams(false, true, tableType, table))
 * </pre>
 */
public class ExprToExternalSqlVisitor extends ExprToSqlVisitor {

    public static final ExprToExternalSqlVisitor INSTANCE = new ExprToExternalSqlVisitor();

    private ExprToExternalSqlVisitor() {
        // singleton
    }

    @Override
    public String visit(Expr expr, ToSqlParams context) {
        throw new UnsupportedOperationException("ExprToExternalSqlVisitor does not support Expr type: "
                + expr.getClass().getSimpleName());
    }

    @Override
    public String visitSlotRef(SlotRef expr, ToSqlParams context) {
        if (expr.getCol() != null) {
            if (context.tableType.equals(TableIf.TableType.JDBC_EXTERNAL_TABLE)
                    || context.tableType.equals(TableIf.TableType.JDBC)) {
                if (context.table instanceof JdbcExternalTable) {
                    return ((JdbcExternalTable) context.table).getProperRemoteColumnName(
                            ((JdbcExternalTable) context.table).getJdbcTableType(), expr.getCol());
                } else {
                    return expr.getCol();
                }
            } else {
                return expr.getCol();
            }
        } else {
            return "<slot " + expr.desc.getId().asInt() + ">";
        }
    }

    @Override
    public String visitCastExpr(CastExpr expr, ToSqlParams context) {
        return expr.getChild(0).accept(this, context);
    }

    @Override
    public String visitTryCastExpr(TryCastExpr expr, ToSqlParams context) {
        return expr.getChild(0).accept(this, context);
    }
}
