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

public abstract class EnforceProperty {
    // Definition of property enforcing type for a given operator.
    //
    // Each enforced property is queried in CEngine::FCheckEnfdProps() to
    // determine if enforcers are required, optional, unnecessary or
    // prohibited over an operator given an optimization context of
    // properties required of it.
    //
    // - Required: operator cannot deliver the required properties on its
    // own, e.g., requiring a sort order from a table scan
    //
    // - Optional: operator can request the required properties from its children
    // and preserve them, e.g., requiring a sort order from a filter
    //
    // - Prohibited: operator prohibits enforcing the required properties on its
    // output, e.g., requiring a sort order on column A from a sort operator that
    // provides sorting on column B
    //
    // - Unnecessary: operator already establishes the required properties on its
    // own, e.g., requiring a sort order on column A from a sort operator that
    // provides sorting on column A. If the required property spec is empty, any
    // operator satisfies it so its type falls into this category.
    //
    // NB: 'Prohibited' prevents ANY enforcer to be added for the given
    // operator & optimization context, even if one is required by some other
    // enforced property.
    public enum EnforceType {
        REQUIRED,
        OPTIONAL,
        PROHIBITED,
        UNNECESSARY
    }

    // Return true if we can add this enforcer, otherwise return false
    public static boolean isEnforceable(EnforceType type) {
        return type == EnforceType.REQUIRED || type == EnforceType.OPTIONAL;
    }

    // check if this type is already optimized. Return true means this is optimized, and we
    public static boolean isOptimized(EnforceType type) {
        return type == EnforceType.OPTIONAL || type == EnforceType.UNNECESSARY;
    }

    public abstract OptPropertySpec getPropertySpec();

    // append enforcers to an expression, new generated expressions will be added into expressions
    public void appendEnforcers(EnforceType type,
                                RequiredPhysicalProperty reqdProp,
                                OptExpression child,
                                OptExpressionHandle exprHandle,
                                List<OptExpression> expressions) {
        if (isEnforceable(type)) {
            getPropertySpec().appendEnforcers(reqdProp, child, exprHandle, expressions);
        }
    }
}
