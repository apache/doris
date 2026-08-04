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

import org.apache.doris.common.UserException;
import org.apache.doris.system.Backend;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/** Tests the mixed-version safety gate for plugin-driven Variant scans. */
public class PluginDrivenScanNodeCompatibilityTest {

    @Test
    public void computeVariantRejectsSmoothUpgradeSourceBackend() {
        Backend backend = new Backend(7L, "127.0.0.1", 9050);
        backend.setSmoothUpgradeSrc(true);

        UserException exception = Assert.assertThrows(UserException.class,
                () -> PluginDrivenScanNode.checkVariantBackendCompatibility(
                        true, Collections.singletonList(backend)));
        Assert.assertTrue(exception.getMessage().contains("backend 7"));
    }

    @Test
    public void compatibilityCheckIgnoresScansWithoutComputeVariant() throws UserException {
        Backend backend = new Backend(7L, "127.0.0.1", 9050);
        backend.setSmoothUpgradeSrc(true);

        PluginDrivenScanNode.checkVariantBackendCompatibility(
                false, Collections.singletonList(backend));
    }
}
