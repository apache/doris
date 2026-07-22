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

import org.apache.doris.foundation.property.StoragePropertiesException;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** Phase A5 golden tests freezing current fe-core OzoneProperties behaviour. */
public class OzonePropertiesParityTest {

    private static Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.ozone.support", "true");
        props.put("ozone.endpoint", "http://127.0.0.1:9878");
        props.put("ozone.access_key", "myAk");
        props.put("ozone.secret_key", "mySk");
        return props;
    }

    @Test
    public void testIdentity() {
        StorageProperties sp = StorageProperties.createPrimary(baseProps());
        Assertions.assertTrue(sp instanceof OzoneProperties);
        Assertions.assertEquals(StorageProperties.Type.OZONE, sp.getType());
        Assertions.assertEquals("S3", sp.getStorageName());
        Assertions.assertEquals(ImmutableSet.of("s3", "s3a", "s3n"), ((OzoneProperties) sp).schemas());
    }

    @Test
    public void testBackendMapExactPathStyleDefaultsTrue() {
        StorageProperties sp = StorageProperties.createPrimary(baseProps());
        ParityAsserts.assertExactMap(ParityAsserts.map(
                "AWS_ENDPOINT", "http://127.0.0.1:9878",
                "AWS_REGION", "us-east-1",
                "AWS_ACCESS_KEY", "myAk",
                "AWS_SECRET_KEY", "mySk",
                "AWS_MAX_CONNECTIONS", "100",
                "AWS_REQUEST_TIMEOUT_MS", "10000",
                "AWS_CONNECTION_TIMEOUT_MS", "10000",
                "use_path_style", "true"
        ), sp.getBackendConfigProperties());
    }

    @Test
    public void testNoGuessWithoutExplicitSupport() {
        // Ozone is the only provider with no guessIsMe wiring in the registry:
        // without fs.ozone.support=true, createPrimary must NOT return
        // OzoneProperties, whatever else it routes to.
        Map<String, String> props = new HashMap<>();
        props.put("ozone.endpoint", "http://127.0.0.1:9878");
        props.put("ozone.access_key", "myAk");
        props.put("ozone.secret_key", "mySk");
        try {
            StorageProperties sp = StorageProperties.createPrimary(props);
            Assertions.assertFalse(sp instanceof OzoneProperties,
                    "ozone must require explicit fs.ozone.support=true, got: " + sp.getClass());
        } catch (StoragePropertiesException e) {
            // also acceptable: nothing matches at all
        }
    }
}
