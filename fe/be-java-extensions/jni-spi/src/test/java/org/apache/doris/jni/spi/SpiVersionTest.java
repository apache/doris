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

package org.apache.doris.jni.spi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SpiVersionTest {

    /**
     * The version BE serves is produced by maven resource filtering. If filtering is not configured
     * the resource still ships, carrying the literal "${jni.plugin.api.version}" - a build defect
     * that would otherwise only show up as every plugin being rejected at runtime.
     */
    @Test
    void theVersionResourceIsFilteredAtBuildTime() {
        String version = SpiVersion.version();
        Assertions.assertFalse(version.contains("${"), "unfiltered placeholder in the version resource: " + version);
        Assertions.assertTrue(SpiVersion.major() >= 1, "major must be a positive number, got: " + version);
    }

    /**
     * Only the major is compared, because "major" is defined as any change to the SPI surface. A
     * parser that accepted a trailing minor as part of the major would silently make 1.0 and 10.0
     * look related.
     */
    @Test
    void onlyTheMajorComponentIsCompared() {
        Assertions.assertEquals(1, SpiVersion.majorOf("1.0"));
        Assertions.assertEquals(1, SpiVersion.majorOf("1.7"));
        Assertions.assertEquals(1, SpiVersion.majorOf(" 1.0 "));
        Assertions.assertEquals(1, SpiVersion.majorOf("1"));
        Assertions.assertEquals(10, SpiVersion.majorOf("10.0"));
    }

    /**
     * A plugin that declares nothing, or garbage, must be rejected rather than admitted. The loader
     * distinguishes the two cases by this -1, so it must never come back as a usable major.
     */
    @Test
    void anUndeclaredOrMalformedVersionIsNotAMajor() {
        Assertions.assertEquals(-1, SpiVersion.majorOf(null));
        Assertions.assertEquals(-1, SpiVersion.majorOf(""));
        Assertions.assertEquals(-1, SpiVersion.majorOf("   "));
        Assertions.assertEquals(-1, SpiVersion.majorOf("v1.0"));
        Assertions.assertEquals(-1, SpiVersion.majorOf("1x.0"));
        Assertions.assertEquals(-1, SpiVersion.majorOf(".5"));
    }
}
