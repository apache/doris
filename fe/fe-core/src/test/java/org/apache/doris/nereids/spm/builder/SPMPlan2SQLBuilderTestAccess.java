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

package org.apache.doris.nereids.spm.builder;

import java.lang.reflect.Method;

/** White-box access to {@link SPMPlan2SQLBuilder} internals for unit tests. */
public final class SPMPlan2SQLBuilderTestAccess {

    private SPMPlan2SQLBuilderTestAccess() {
    }

    /** Whether the inner Limit exactly continues the outer one (offset preserved, no slack). */
    public static boolean isExactTopNContinuationLimit(long outerLimit, long outerOffset, long innerLimit) {
        try {
            Method m = SPMPlan2SQLBuilder.class.getDeclaredMethod("isExactTopNContinuationLimit",
                    long.class, long.class, long.class);
            m.setAccessible(true);
            return (Boolean) m.invoke(null, outerLimit, outerOffset, innerLimit);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }
}
