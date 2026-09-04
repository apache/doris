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

package org.apache.doris.filesystem.properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

class FsCacheKeysTest {

    private static Map<String, String> props(String... kv) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put(kv[i], kv[i + 1]);
        }
        return map;
    }

    @Test
    void fingerprintIsStableAndOrderIndependent() {
        Map<String, String> ordered = new LinkedHashMap<>();
        ordered.put("a", "1");
        ordered.put("b", "2");
        Map<String, String> reversed = new LinkedHashMap<>();
        reversed.put("b", "2");
        reversed.put("a", "1");

        Assertions.assertEquals(FsCacheKeys.fingerprintOf("salt", ordered),
                FsCacheKeys.fingerprintOf("salt", reversed));
        Assertions.assertNotEquals(FsCacheKeys.fingerprintOf("salt", ordered),
                FsCacheKeys.fingerprintOf("other", ordered));
    }

    @Test
    void embeddedSeparatorsCannotForgeAnotherPropertySet() {
        // A delimiter-joined encoding ("\nkey=value") is not injective: because both names and
        // values are caller-controlled, one value carrying embedded newlines reproduces the exact
        // bytes of several separate entries. The raw fs.* overlay hands the first map to Hadoop as
        // a single ignored value and the second as real S3A credentials, so an equal fingerprint
        // would let them share one cached FileSystem.
        Map<String, String> smuggled = props("fs.ignored",
                "\nfs.s3a.access.key=AK"
                        + "\nfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
                        + "\nfs.s3a.secret.key=SK");
        Map<String, String> real = props(
                "fs.ignored", "",
                "fs.s3a.access.key", "AK",
                "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "fs.s3a.secret.key", "SK");

        Assertions.assertNotEquals(FsCacheKeys.fingerprintOf("salt", smuggled),
                FsCacheKeys.fingerprintOf("salt", real));
    }

    @Test
    void keyValueBoundaryIsUnambiguous() {
        // "k=" + "v" must not encode the same as "k" + "=v".
        Assertions.assertNotEquals(FsCacheKeys.fingerprintOf("salt", props("fs.a=", "b")),
                FsCacheKeys.fingerprintOf("salt", props("fs.a", "=b")));
        // ... nor may a value's own '=' merge into the next entry.
        Assertions.assertNotEquals(FsCacheKeys.fingerprintOf("salt", props("fs.a", "1", "fs.b", "2")),
                FsCacheKeys.fingerprintOf("salt", props("fs.a", "1\nfs.b=2")));
    }

    @Test
    void nullValueIsDistinctFromEmptyValue() {
        Map<String, String> nullValued = new HashMap<>();
        nullValued.put("fs.a", null);

        Assertions.assertNotEquals(FsCacheKeys.fingerprintOf("salt", nullValued),
                FsCacheKeys.fingerprintOf("salt", props("fs.a", "")));
    }

    @Test
    void saltIsFramedToo() {
        // A salt (the concrete class name) must not be able to absorb the first entry.
        Assertions.assertNotEquals(FsCacheKeys.fingerprintOf("a", props("b", "c")),
                FsCacheKeys.fingerprintOf("a\nb=c", new HashMap<>()));
    }

    @Test
    void fingerprintHasTheDocumentedShape() {
        String fingerprint = FsCacheKeys.fingerprintOf("salt", props("fs.a", "1"));
        Assertions.assertEquals(32, fingerprint.length());
        Assertions.assertTrue(fingerprint.matches("[0-9a-f]{32}"), fingerprint);
    }

    @Test
    void fsCacheKeyPropertyIsPerSchemeAndLowerCased() {
        Assertions.assertEquals("doris.fs.cache.key.s3a", FsCacheKeys.fsCacheKeyProperty("S3A"));
        Assertions.assertNotEquals(FsCacheKeys.FS_CACHE_KEY_PROPERTY, FsCacheKeys.fsCacheKeyProperty("hdfs"));
    }
}
