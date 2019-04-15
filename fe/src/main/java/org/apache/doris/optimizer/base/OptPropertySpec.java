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

package org.apache.doris.optimizer.base;

import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.OptExpressionHandle;

import java.util.List;

public interface OptPropertySpec {

    /**
     * This function will add enforced Expression to 'expressions'.
     * @param reqdProp contains property need to be enforced
     * @param child is the expression that enforce will add on
     * @param exprHandle contains information for
     * @param expressions
     */
    void appendEnforcers(
            RequiredPhysicalProperty reqdProp,
            OptExpression child,
            OptExpressionHandle exprHandle,
            List<OptExpression> expressions);

    /**
     * Whether this object is satisfied with spec.
     * @param spec
     * @return
     */
    boolean isSatisfy(OptPropertySpec spec);
}
