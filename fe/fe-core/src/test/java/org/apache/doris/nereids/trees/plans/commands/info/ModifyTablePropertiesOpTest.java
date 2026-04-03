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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.catalog.DataProperty.MediumAllocationMode;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ModifyTablePropertiesOpTest {

    @Test
    public void testMediumAllocationModeStrict() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "strict");
        ModifyTablePropertiesOp op = new ModifyTablePropertiesOp(props);
        op.validate(null);
        Assertions.assertEquals(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC, op.getOpType());
        Assertions.assertEquals(MediumAllocationMode.STRICT, op.getMediumAllocationMode());
    }

    @Test
    public void testMediumAllocationModeAdaptive() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "adaptive");
        ModifyTablePropertiesOp op = new ModifyTablePropertiesOp(props);
        op.validate(null);
        Assertions.assertEquals(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC, op.getOpType());
        Assertions.assertEquals(MediumAllocationMode.ADAPTIVE, op.getMediumAllocationMode());
    }

    @Test
    public void testMediumAllocationModeInvalid() {
        Map<String, String> props = new HashMap<>();
        props.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "invalid_mode");
        ModifyTablePropertiesOp op = new ModifyTablePropertiesOp(props);
        Assertions.assertThrows(AnalysisException.class, () -> op.validate(null));
    }

    @Test
    public void testStorageMedium() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "HDD");
        ModifyTablePropertiesOp op = new ModifyTablePropertiesOp(props);
        op.validate(null);
        Assertions.assertEquals(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC, op.getOpType());
    }
}
