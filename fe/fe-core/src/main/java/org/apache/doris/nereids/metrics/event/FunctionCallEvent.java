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

package org.apache.doris.nereids.metrics.event;

import org.apache.doris.nereids.metrics.Event;
import org.apache.doris.nereids.util.Utils;

/**
 * function call event
 */
public class FunctionCallEvent extends Event {
    private final String callFuncNameAndLine;

    private FunctionCallEvent(String callFuncNameAndLine) {
        this.callFuncNameAndLine = callFuncNameAndLine;
    }

    public static FunctionCallEvent of(String callFuncNameAndLine) {
        return checkConnectContext(FunctionCallEvent.class) ? new FunctionCallEvent(callFuncNameAndLine) : null;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("FunctionCallEvent", "callFuncNameAndLine='", callFuncNameAndLine);
    }
}
