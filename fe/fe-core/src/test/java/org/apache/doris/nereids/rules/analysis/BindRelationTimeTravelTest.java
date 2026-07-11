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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TableSnapshot.VersionType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for TableSnapshot factory methods used by the FOR SYSTEM_TIME AS OF
 * syntax in BindRelation. Cloud-mode validation tests (timestamp bounds, retention
 * window, TT-enabled table checks) are covered by the cloud regression tests in
 * regression-test/suites/cloud_p0/time_travel/.
 */
class BindRelationTimeTravelTest {

    @Test
    void systemTimeOf_createsSystemTimeType() {
        TableSnapshot snap = TableSnapshot.systemTimeOf("2024-01-15 10:00:00");
        Assertions.assertEquals(VersionType.SYSTEM_TIME, snap.getType());
        Assertions.assertEquals("2024-01-15 10:00:00", snap.getValue());
    }

    @Test
    void timeOf_createsTimeType() {
        TableSnapshot snap = TableSnapshot.timeOf("2024-01-15 10:00:00");
        Assertions.assertEquals(VersionType.TIME, snap.getType());
    }

    @Test
    void versionOf_createsVersionType() {
        TableSnapshot snap = TableSnapshot.versionOf("5");
        Assertions.assertEquals(VersionType.VERSION, snap.getType());
        Assertions.assertEquals("5", snap.getValue());
    }

    @Test
    void systemTimeOf_toString_containsCorrectSyntax() {
        TableSnapshot snap = TableSnapshot.systemTimeOf("2024-01-15 10:00:00");
        Assertions.assertTrue(snap.toString().contains("FOR SYSTEM_TIME AS OF"),
                "toString must produce FOR SYSTEM_TIME AS OF, got: " + snap.toString());
    }

    @Test
    void timeOf_toString_containsCorrectSyntax() {
        TableSnapshot snap = TableSnapshot.timeOf("2024-01-15 10:00:00");
        Assertions.assertTrue(snap.toString().contains("FOR TIME AS OF"),
                "toString must produce FOR TIME AS OF, got: " + snap.toString());
    }

    @Test
    void versionOf_toString_containsCorrectSyntax() {
        TableSnapshot snap = TableSnapshot.versionOf("5");
        Assertions.assertTrue(snap.toString().contains("FOR VERSION AS OF"),
                "toString must produce FOR VERSION AS OF, got: " + snap.toString());
    }

    @Test
    void systemTimeOf_toDigest_usesPlaceholder() {
        TableSnapshot snap = TableSnapshot.systemTimeOf("2024-01-15 10:00:00");
        String digest = snap.toDigest();
        Assertions.assertTrue(digest.contains("FOR SYSTEM_TIME AS OF"),
                "toDigest must contain FOR SYSTEM_TIME AS OF, got: " + digest);
        Assertions.assertTrue(digest.contains("?"),
                "toDigest must use ? placeholder for the value, got: " + digest);
    }

    @Test
    void systemTimeType_isDistinctFromTimeType() {
        Assertions.assertNotEquals(VersionType.SYSTEM_TIME, VersionType.TIME,
                "SYSTEM_TIME and TIME must be distinct types for correct routing");
    }
}
