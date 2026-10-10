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

package org.apache.doris.datasource.scan;

import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.Backend;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public class PluginDrivenScanNodeBackendCapabilityTest {
    private static final String PAIMON_RUST_READER = "paimon-rust-reader";

    @Test
    public void emptyBackendSetAdvertisesNoCapabilities() {
        Assertions.assertEquals(Collections.emptySet(),
                PluginDrivenScanNode.commonBackendCapabilities(Collections.emptyList()));
    }

    @Test
    public void capabilityMustBeSupportedByEveryEligibleBackend() {
        Backend capable1 = capableBackend(1);
        Backend capable2 = capableBackend(2);
        Backend legacy = new Backend(3, "127.0.0.1", 9050);

        Assertions.assertEquals(Collections.singleton(PAIMON_RUST_READER),
                PluginDrivenScanNode.commonBackendCapabilities(Collections.singletonList(capable1)));
        Assertions.assertEquals(Collections.singleton(PAIMON_RUST_READER),
                PluginDrivenScanNode.commonBackendCapabilities(Arrays.asList(capable1, capable2)));
        Assertions.assertEquals(Collections.emptySet(),
                PluginDrivenScanNode.commonBackendCapabilities(Arrays.asList(capable1, legacy)));
    }

    @Test
    public void returnedCapabilitiesAreIndependentOfBackendSets() {
        Backend capable = capableBackend(1);
        Set<String> common = PluginDrivenScanNode.commonBackendCapabilities(
                Collections.singletonList(capable));

        Assertions.assertTrue(common.remove(PAIMON_RUST_READER));
        Assertions.assertTrue(capable.getConnectorCapabilities().contains(PAIMON_RUST_READER));
    }

    private static Backend capableBackend(long id) {
        return GsonUtils.GSON.fromJson(
                "{\"id\":" + id + ",\"supportsPaimonRustReader\":true}", Backend.class);
    }
}
