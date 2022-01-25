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

package org.apache.doris.utframe;

import org.apache.doris.common.Config;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UtFrameUtilsTest {

    @Test
    public void testCreateDorisCluster() throws Exception {
        String runningDir = UtFrameUtils.generateRandomFeRunningDir(UtFrameUtilsTest.class);
        assertTrue(runningDir.startsWith("fe/mocked/UtFrameUtilsTest/"));
        Map<String, String> feConfMap = new HashMap<>();
        feConfMap.put("cluster_name", "Doris Test");
        feConfMap.put("enable_complex_type_support", "true");
        UtFrameUtils.createDorisCluster(runningDir, feConfMap);
        assertEquals("Doris Test", Config.cluster_name);
        assertTrue(Config.enable_complex_type_support);
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }
}
