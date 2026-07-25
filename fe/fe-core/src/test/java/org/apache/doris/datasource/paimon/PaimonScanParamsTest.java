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

package org.apache.doris.datasource.paimon;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

public class PaimonScanParamsTest {

    @Test
    public void testValidateKnownScanOptions() {
        PaimonScanParams.validateOptions(ImmutableMap.of(
                "scan.snapshot-id", "1",
                "scan.plan-sort-partition", "true"));
    }

    @Test
    public void testRejectUnknownAndConflictingOptions() {
        IllegalArgumentException typo = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonScanParams.validateOptions(
                        ImmutableMap.of("scan.snapsh0t-id", "1")));
        Assert.assertTrue(typo.getMessage().contains("scan.snapsh0t-id"));

        IllegalArgumentException conflict = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonScanParams.validateOptions(ImmutableMap.of(
                        "scan.snapshot-id", "1",
                        "scan.tag-name", "tag1")));
        Assert.assertTrue(conflict.getMessage().contains("Only one"));
    }
}
