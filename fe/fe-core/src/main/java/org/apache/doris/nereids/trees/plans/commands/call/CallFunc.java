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

package org.apache.doris.nereids.trees.plans.commands.call;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.nereids.analyzer.UnboundFunction;

/**
 * call function
 */
public abstract class CallFunc {

    /**
     * Get the instance of CallFunc
     */
    public static CallFunc getFunc(UserIdentity user, UnboundFunction unboundFunction) {
        String funcName = unboundFunction.getName().toUpperCase();
        switch (funcName) {
            case "EXECUTE_STMT":
                return CallExecuteStmtFunc.create(user, unboundFunction.getArguments());
            default:
                throw new IllegalArgumentException("unknown function name: " + funcName);
        }
    }

    public abstract void run();
}
