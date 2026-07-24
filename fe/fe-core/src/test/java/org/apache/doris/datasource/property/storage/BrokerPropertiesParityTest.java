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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** Phase A5 golden tests freezing current fe-core BrokerProperties behaviour. */
public class BrokerPropertiesParityTest {

    @Test
    public void testIdentityAndParams() {
        Map<String, String> props = new HashMap<>();
        props.put("broker.name", "hdfs_broker");
        props.put("broker.username", "u1");
        props.put("broker.password", "p1");
        props.put("unrelated.key", "x");
        StorageProperties sp = StorageProperties.createPrimary(props);
        Assertions.assertTrue(sp instanceof BrokerProperties);
        Assertions.assertEquals(StorageProperties.Type.BROKER, sp.getType());
        Assertions.assertEquals("BROKER", sp.getStorageName());
        Assertions.assertNull(sp.getHadoopStorageConfig());
        // backend map IS the original props (same content)
        Assertions.assertEquals(props, sp.getBackendConfigProperties());
        // brokerParams strips the "broker." prefix — including broker.name -> name
        BrokerProperties bp = (BrokerProperties) sp;
        Assertions.assertEquals("hdfs_broker", bp.getBrokerName());
        ParityAsserts.assertExactMap(ParityAsserts.map(
                "name", "hdfs_broker",
                "username", "u1",
                "password", "p1"
        ), bp.getBrokerParams());
    }
}
