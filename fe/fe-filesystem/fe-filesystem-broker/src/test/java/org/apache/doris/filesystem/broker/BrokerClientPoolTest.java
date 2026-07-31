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

package org.apache.doris.filesystem.broker;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

/**
 * Verifies the tuned pool defaults from finding F16: per-key cap of 64 and 5&nbsp;s
 * borrow timeout. These guard against runaway plans exhausting the broker process and
 * spurious borrow failures under modest contention.
 */
class BrokerClientPoolTest {

    @Test
    void poolDefaults_useTunedValues() throws Exception {
        BrokerClientPool clientPool = new BrokerClientPool();
        try {
            Field poolField = BrokerClientPool.class.getDeclaredField("pool");
            poolField.setAccessible(true);
            GenericKeyedObjectPool<?, ?> pool = (GenericKeyedObjectPool<?, ?>) poolField.get(clientPool);

            Assertions.assertEquals(64, pool.getMaxTotalPerKey(),
                    "maxTotalPerKey must cap broker connections per host");
            Assertions.assertEquals(5_000L, pool.getMaxWaitMillis(),
                    "maxWaitMillis must be 5s to absorb modest contention");
            // Sanity check on previously established defaults that remain unchanged.
            Assertions.assertTrue(pool.getTestOnBorrow(),
                    "testOnBorrow must remain enabled to detect dead sockets");
        } finally {
            clientPool.close();
        }
    }
}
