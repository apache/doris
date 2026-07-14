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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.UserException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class StoragePropertiesFsCacheFingerprintTest {

    private static StorageProperties hdfs(String user) throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "hdfs://test/1.orc");
        props.put("hadoop.username", user);
        return StorageProperties.createPrimary(props);
    }

    @Test
    public void testFingerprintStableForSameDefinition() throws UserException {
        Assertions.assertEquals(hdfs("userA").getFsCacheFingerprint(), hdfs("userA").getFsCacheFingerprint());
    }

    @Test
    public void testFingerprintDiffersAcrossCredentials() throws UserException {
        Assertions.assertNotEquals(hdfs("userA").getFsCacheFingerprint(), hdfs("userB").getFsCacheFingerprint());
    }

    @Test
    public void testBackendConfigCarriesFingerprint() throws UserException {
        StorageProperties sp = hdfs("userA");
        Map<String, String> beProps = sp.getBackendConfigProperties();
        Assertions.assertEquals(sp.getFsCacheFingerprint(), beProps.get(StorageProperties.FS_CACHE_KEY_PROPERTY));
    }

    @Test
    public void testHadoopStorageConfigCarriesFingerprint() throws UserException {
        StorageProperties sp = hdfs("userA");
        Assertions.assertEquals(sp.getFsCacheFingerprint(),
                sp.getHadoopStorageConfig().get(StorageProperties.FS_CACHE_KEY_PROPERTY));
    }

    @Test
    public void testCombinedFingerprintOrderIndependent() throws UserException {
        StorageProperties a = hdfs("userA");
        StorageProperties b = hdfs("userB");
        String ab = StorageProperties.combinedFsCacheFingerprint(Arrays.asList(a, b));
        String ba = StorageProperties.combinedFsCacheFingerprint(Arrays.asList(b, a));
        Assertions.assertEquals(ab, ba);
        // A different member set must yield a different combined fingerprint.
        Assertions.assertNotEquals(ab, StorageProperties.combinedFsCacheFingerprint(Arrays.asList(a, a)));
    }

    @Test
    public void testCombinedFingerprintOfSingleStorageIsItsOwn() throws UserException {
        StorageProperties a = hdfs("userA");
        Assertions.assertEquals(a.getFsCacheFingerprint(),
                StorageProperties.combinedFsCacheFingerprint(Arrays.asList(a)));
    }
}
