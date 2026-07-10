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

package org.apache.doris.planner;

import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.computegroup.ComputeGroup;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OlapScanNodeBackendSelectionConfigTest {
    @Test
    void testResourceTagLocationCheckConfigGate() {
        boolean oldConfig = Config.resource_tag_location_check;
        try {
            Config.resource_tag_location_check = false;
            Assertions.assertTrue(OlapScanNode.shouldFilterReplicaByResourceTag(
                    true, true, ComputeGroup.INVALID_COMPUTE_GROUP, "rg_a"));
            ComputeGroup computeGroup = new ComputeGroup("rg_a", "rg_a", null);
            Assertions.assertFalse(OlapScanNode.shouldFilterReplicaByResourceTag(false, true, computeGroup, "rg_b"));

            Config.resource_tag_location_check = true;
            Assertions.assertTrue(OlapScanNode.shouldFilterReplicaByResourceTag(
                    true, false, ComputeGroup.INVALID_COMPUTE_GROUP, "rg_a"));
            Assertions.assertTrue(OlapScanNode.shouldFilterReplicaByResourceTag(false, true, computeGroup, "rg_b"));
            Assertions.assertFalse(OlapScanNode.shouldFilterReplicaByResourceTag(false, true, computeGroup, "rg_a"));
        } finally {
            Config.resource_tag_location_check = oldConfig;
        }
    }

    @Test
    void testQuerySelectionDisabledInCloudMode() {
        String oldCloudUniqueId = Config.cloud_unique_id;
        String oldDeployMode = Config.deploy_mode;
        try {
            Config.cloud_unique_id = "cloud_id";
            Config.deploy_mode = "cloud";
            Assertions.assertFalse(OlapScanNode.shouldApplyQuerySelection(false));
            Assertions.assertFalse(OlapScanNode.shouldApplyQuerySelection(true));

            Config.cloud_unique_id = "";
            Config.deploy_mode = "";
            Assertions.assertTrue(OlapScanNode.shouldApplyQuerySelection(false));
            Assertions.assertFalse(OlapScanNode.shouldApplyQuerySelection(true));
        } finally {
            Config.cloud_unique_id = oldCloudUniqueId;
            Config.deploy_mode = oldDeployMode;
        }
    }

    @Test
    void testRequiredQuerySelectionRejectsBypassModes() {
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "key_a", BackendSelection.Mode.REQUIRE, "test");

        Assertions.assertThrows(UserException.class,
                () -> OlapScanNode.validateRequiredQuerySelection(true, -1, hint));
        Assertions.assertThrows(UserException.class,
                () -> OlapScanNode.validateRequiredQuerySelection(false, 0, hint));
        Assertions.assertDoesNotThrow(
                () -> OlapScanNode.validateRequiredQuerySelection(false, -1, hint));
    }
}
