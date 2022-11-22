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

package org.apache.doris.nereids.metrics;

import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

/**
 * event
 */
public abstract class Event implements Cloneable {
    private final long stateId;

    protected Event() {
        this.stateId = -1;
    }

    protected Event(long stateId) {
        this.stateId = stateId;
    }

    protected static boolean checkConnectContext(Class<? extends Event> targetClass) {
        return ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable().isEnableNereidsTrace()
                && ConnectContext.get().getSessionVariable().getParsedNereidsEventMode().contains(targetClass);
    }

    public final String toJson() {
        return Utils.toSqlString("Event", "StateId", stateId);
    }

    @Override
    public String toString() {
        return toJson();
    }

    @Override
    public Event clone() throws CloneNotSupportedException {
        return (Event) super.clone();
    }
}
