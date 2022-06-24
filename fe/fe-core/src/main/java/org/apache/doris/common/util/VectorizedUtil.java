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

import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.DdlException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

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
        return connectContext.getSessionVariable().enableVectorizedEngine();
    }

    public static void switchToQueryNonVec() {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            return;
        }
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.setIsSingleSetVar(true);
        try {
            VariableMgr.setVar(sessionVariable, new SetVar("enable_vectorized_engine", new StringLiteral("false")));
        } catch (DdlException e) {
            // do nothing
        }
    }
}

