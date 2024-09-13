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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.qe.ResultSet;

import java.util.List;
import java.util.Optional;

/**
  * <p>
  * This class is used to return result set in fe without send fragment to be.
  * Some plans support this function, for example:
  * <li>1. the sql `select 100` will generate a plan, PhysicalOneRowRelation, and PhysicalOneRowRelation implement this
  *     interface, so fe can send the only row to client immediately.
  * </li>
  * <li>2. the sql `select * from tbl limit 0` will generate PhysicalEmptyRelation, which means no any rows returned,
  *    the PhysicalEmptyRelation implement this interface.
  * </li>
  * </p>
  * <p>
  * If you want to cache the result set in fe, you can implement this interface and write this code:
  * </p>
  * <pre>
  * StatementContext statementContext = cascadesContext.getStatementContext();
  * boolean enableSqlCache
  *         = CacheAnalyzer.canUseSqlCache(statementContext.getConnectContext().getSessionVariable());
  * if (sqlCacheContext.isPresent() && enableSqlCache) {
  *     sqlCacheContext.get().setResultSetInFe(resultSet);
  *     Env.getCurrentEnv().getSqlCacheManager().tryAddFeSqlCache(
  *             statementContext.getConnectContext(),
  *             statementContext.getOriginStatement().originStmt
  *     );
  * }
  * </pre>
  */
public interface ComputeResultSet {
    Optional<ResultSet> computeResultInFe(CascadesContext cascadesContext, Optional<SqlCacheContext> sqlCacheContext,
            List<Slot> outputSlots);
}
