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

package org.apache.doris.common.util;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

public class VectorizedUtil {
    /**
     * 1. Return false if there is no current connection (Rule1 to be changed)
     * 2. Returns the vectorized switch value of the query 'globalState.enableQueryVec'
     * 3. If it is not currently a query, return the vectorized switch value of the session 'enableVectorizedEngine'
     * @return true: vec. false: non-vec
     */
    public static boolean isVectorized() {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            return false;
        }
        StmtExecutor stmtExecutor = connectContext.getExecutor();
        if (stmtExecutor == null) {
            return connectContext.getSessionVariable().enableVectorizedEngine();
        }
        Analyzer analyzer = stmtExecutor.getAnalyzer();
        if (analyzer == null) {
            return connectContext.getSessionVariable().enableVectorizedEngine();
        }
        return analyzer.enableQueryVec();
    }

    /**
     * The purpose of this function is to turn off the vectorization switch for the current query.
     * When the vectorization engine cannot meet the requirements of the current query,
     * it will convert the current query into a non-vectorized query.
     * Note that this will only change the **vectorization switch for a single query**,
     * and will not affect other queries in the same session.
     * Therefore, even if the vectorization switch of the current query is turned off,
     * the vectorization properties of subsequent queries will not be affected.
     *
     * Session: set enable_vectorized_engine=true;
     * Query1: select * from table (vec)
     * Query2: select * from t1 left join (select count(*) as count from t2) t3 on t1.k1=t3.count (switch to non-vec)
     * Query3: select * from table (still vec)
     */
    public static void switchToQueryNonVec() {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            return;
        }
        Analyzer analyzer = connectContext.getExecutor().getAnalyzer();
        if (analyzer == null) {
            return;
        }
        analyzer.disableQueryVec();
    }
}

