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

package org.apache.doris.nereids.rules.exploration;


/**
 * JoinReorderContext for Duplicate free.
 * Paper: Improving Join Reorderability with Compensation Operators
 */
public class JoinReorderContext {
    // left deep tree
    private boolean hasCommute = false;
    private boolean hasTopPushThrough = false;

    public JoinReorderContext() {
    }

    void copyFrom(JoinReorderContext joinReorderContext) {
        this.hasCommute = joinReorderContext.hasCommute;
        this.hasTopPushThrough = joinReorderContext.hasTopPushThrough;
    }

    JoinReorderContext copy() {
        JoinReorderContext joinReorderContext = new JoinReorderContext();
        joinReorderContext.copyFrom(this);
        return joinReorderContext;
    }

    public boolean isHasCommute() {
        return hasCommute;
    }

    public boolean isHasTopPushThrough() {
        return hasTopPushThrough;
    }
}
