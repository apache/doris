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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class UiCapabilityResolverTest {
    @Test
    void ordinaryUsersReceiveOnlyUserCapabilities() {
        Assertions.assertEquals(
                Arrays.asList(UiCapability.PLAYGROUND_USE, UiCapability.QUERY_PROFILE_VIEW_OWN),
                UiCapabilityResolver.resolve(false, false));
    }

    @Test
    void adminOrNodeUsersReceiveOperationalCapabilities() {
        List<UiCapability> capabilities = UiCapabilityResolver.resolve(true, false);

        Assertions.assertTrue(capabilities.contains(UiCapability.NODE_STATUS_VIEW));
        Assertions.assertTrue(capabilities.contains(UiCapability.OPERATIONS_VIEW));
        Assertions.assertTrue(capabilities.contains(UiCapability.LOG_MODIFY));
        Assertions.assertFalse(capabilities.contains(UiCapability.CONFIGURATION_MODIFY));
    }

    @Test
    void administratorsReceiveAllAdminCapabilities() {
        List<UiCapability> capabilities = UiCapabilityResolver.resolve(true, true);

        Assertions.assertEquals(UiCapability.values().length, capabilities.size());
        Assertions.assertTrue(capabilities.contains(UiCapability.QUERY_PROFILE_VIEW_ALL));
        Assertions.assertTrue(capabilities.contains(UiCapability.CONFIGURATION_MODIFY));
    }
}
