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

package org.apache.doris.system;

import org.apache.commons.lang.StringUtils;

public class BackendEvent {

    public enum BackendEventType {
        BACKEND_DOWN,
        BACKEND_DROPPED,
        BACKEND_DECOMMISSION
    }

    private final BackendEventType type;
    private final String message;
    private final Long[] backendIds;

    public BackendEvent(BackendEventType type, String message, Long...backendIds) {
        this.type = type;
        this.message = message;
        this.backendIds = backendIds;
    }

    public BackendEventType getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

    public Long[] getBackendIds() {
        return backendIds;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Backend[").append(StringUtils.join(backendIds, ", ")).append("]");
        sb.append(" Type: ").append(type.name());
        sb.append(" Msg: ").append(message);
        return sb.toString();
    }
}
