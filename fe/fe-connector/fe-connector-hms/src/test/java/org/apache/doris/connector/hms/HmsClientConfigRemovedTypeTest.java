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

package org.apache.doris.connector.hms;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests {@link HmsClientConfig#removedMetastoreTypeError(Map)} — the rejection of metastore types that have
 * been removed ({@code glue}, {@code dlf}).
 *
 * <p>WHY: both removed types used to be selected by {@code hive.metastore.type} and dispatched to a vendored
 * client. The dispatch they were removed from ends in a plain-HMS fallback, so a removed value that is merely
 * absent from the dispatch does NOT fail — it silently connects to whatever plain HMS is reachable while the
 * user believes they configured Glue/DLF. Nothing else covers this, so without these tests the rejection could
 * be deleted and no test would go red.
 *
 * <p>The tests pin both halves of the contract: every removed type is REJECTED with a message that names it,
 * and the surviving type is NOT — a rejection that also caught {@code hms} would be a worse regression than
 * the silent fallthrough it prevents.
 */
public class HmsClientConfigRemovedTypeTest {

    private static Map<String, String> propsWithType(String type) {
        Map<String, String> props = new HashMap<>();
        if (type != null) {
            props.put(HmsClientConfig.METASTORE_TYPE_KEY, type);
        }
        return props;
    }

    private static void assertRejected(String type, String expectedSubject) {
        String error = HmsClientConfig.removedMetastoreTypeError(propsWithType(type));
        Assertions.assertNotNull(error,
                "hive.metastore.type=" + type + " must be rejected, never silently ignored");
        Assertions.assertTrue(error.contains(HmsClientConfig.METASTORE_TYPE_KEY),
                "message must name the offending property, got: " + error);
        Assertions.assertTrue(error.contains(type),
                "message must name the removed type, got: " + error);
        Assertions.assertTrue(error.contains(expectedSubject),
                "message must say WHAT was removed, got: " + error);
        Assertions.assertTrue(error.contains("no longer supported"),
                "message must say the type was removed rather than merely invalid, got: " + error);
    }

    @Test
    public void removedTypesAreRejectedAndNameWhatWasRemoved() {
        // WHY: the message reaches the user verbatim (the catalog layer unwraps it into a DdlException with no
        // prefix), so it must be self-contained and say which feature is gone. MUTATION: dropping either
        // rejection, or emitting a message that never names the type, leaves an error nobody can act on -> red.
        assertRejected("glue", "AWS Glue");
        assertRejected("dlf", "DLF 1.0");
    }

    @Test
    public void rejectionIsCaseInsensitive() {
        // WHY: the dispatch these rejections replace matched with equalsIgnoreCase, so "GLUE"/"Dlf" must not
        // slip through into the plain-HMS fallback. MUTATION: a case-sensitive lookup -> red.
        Assertions.assertNotNull(HmsClientConfig.removedMetastoreTypeError(propsWithType("GLUE")));
        Assertions.assertNotNull(HmsClientConfig.removedMetastoreTypeError(propsWithType("Dlf")));
    }

    @Test
    public void survivingTypeIsNotRejected() {
        // WHY: guards the blast radius of the rejection. MUTATION: rejecting hms (or the absent-type default,
        // which every catalog without an explicit type relies on) would break every Hive catalog -> red.
        Assertions.assertNull(HmsClientConfig.removedMetastoreTypeError(propsWithType(null)),
                "absent type defaults to plain hms and must be accepted");
        Assertions.assertNull(HmsClientConfig.removedMetastoreTypeError(
                propsWithType(HmsClientConfig.METASTORE_TYPE_HMS)));
    }

    @Test
    public void messageAdvertisesOnlyTheSurvivingType() {
        // WHY: the message tells the user what to switch to. Now that dlf is gone too, advertising it would
        // send them at a type that is itself rejected. MUTATION: a stale "Supported types: hms, dlf" -> red.
        String error = HmsClientConfig.removedMetastoreTypeError(propsWithType("glue"));
        Assertions.assertTrue(error.contains("Supported types: " + HmsClientConfig.METASTORE_TYPE_HMS + "."),
                "message must advertise hms as the only supported type, got: " + error);
        Assertions.assertFalse(error.contains("dlf"),
                "message must not advertise the removed dlf type, got: " + error);
    }
}
