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

package org.apache.doris.httpv2.ui;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class UiCapabilityResolver {
    private UiCapabilityResolver() {
    }

    public static List<UiCapability> resolve(boolean adminOrNode, boolean admin) {
        List<UiCapability> capabilities = new ArrayList<>();
        capabilities.add(UiCapability.PLAYGROUND_USE);
        capabilities.add(UiCapability.QUERY_PROFILE_VIEW_OWN);
        if (adminOrNode) {
            capabilities.add(UiCapability.NODE_STATUS_VIEW);
            capabilities.add(UiCapability.OPERATIONS_VIEW);
            capabilities.add(UiCapability.LOG_MODIFY);
        }
        if (admin) {
            capabilities.add(UiCapability.QUERY_PROFILE_VIEW_ALL);
            capabilities.add(UiCapability.CONFIGURATION_MODIFY);
        }
        Collections.sort(capabilities);
        return Collections.unmodifiableList(capabilities);
    }
}
