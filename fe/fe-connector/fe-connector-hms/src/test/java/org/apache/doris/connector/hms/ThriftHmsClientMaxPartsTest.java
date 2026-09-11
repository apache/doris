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

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Tests {@link ThriftHmsClient#toThriftMaxParts}: the connector's {@code maxParts} contract mapped onto the
 * {@code short max_parts} that HMS {@code get_partition_names} accepts.
 *
 * <p>WHY: a non-positive {@code maxParts} means "all partitions" and MUST map to a negative short (HMS reads
 * a negative {@code max_parts} as unbounded). A prior implementation clamped it to {@code Short.MAX_VALUE}
 * (32767), which silently truncated any table with more than 32767 partitions — defeating the {@code -1}
 * "unlimited" contract the hive/hudi partition-listing callers rely on and diverging from the legacy client,
 * which passed {@code Config.max_hive_list_partition_num = -1} straight through. These assertions pin the
 * unbounded mapping so the truncation cannot silently return.</p>
 */
public class ThriftHmsClientMaxPartsTest {

    @Test
    public void testNonPositiveMeansUnbounded() {
        // -1 and 0 both mean "all"; HMS reads a negative short as unbounded.
        Assertions.assertEquals((short) -1, ThriftHmsClient.toThriftMaxParts(-1));
        Assertions.assertEquals((short) -1, ThriftHmsClient.toThriftMaxParts(0));
        // Must NOT be the old silent cap.
        Assertions.assertNotEquals(Short.MAX_VALUE, ThriftHmsClient.toThriftMaxParts(-1));
    }

    @Test
    public void testPositiveWithinShortIsPassedThrough() {
        Assertions.assertEquals((short) 1, ThriftHmsClient.toThriftMaxParts(1));
        Assertions.assertEquals((short) 100, ThriftHmsClient.toThriftMaxParts(100));
        Assertions.assertEquals(Short.MAX_VALUE, ThriftHmsClient.toThriftMaxParts(Short.MAX_VALUE));
    }

    @Test
    public void testPositiveAboveShortNarrowsToUnbounded() {
        // The 100000-cap callers rely on this: (short) 100000 is negative, so HMS treats it as unbounded.
        short mapped = ThriftHmsClient.toThriftMaxParts(100000);
        Assertions.assertEquals((short) 100000, mapped);
        Assertions.assertTrue(mapped < 0, "a value above Short.MAX_VALUE must narrow to a negative (unbounded) short");
    }

    @Test
    public void testFilteredPartitionResponseIsBounded() {
        Assertions.assertFalse(ThriftHmsClient.isFilteredPartitionResponseSaturated(
                HmsClientConfig.DEFAULT_PARTITION_BATCH_SIZE));
        Assertions.assertTrue(ThriftHmsClient.isFilteredPartitionResponseSaturated(
                HmsClientConfig.DEFAULT_PARTITION_BATCH_SIZE + 1));
    }

    @Test
    public void testFilteredPartitionResponseFallsBackWhenSaturated() throws Exception {
        IMetaStoreClient metastore = (IMetaStoreClient) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {IMetaStoreClient.class}, (proxy, method, args) -> {
                    if ("listPartitionsByFilter".equals(method.getName())) {
                        Assertions.assertEquals("db", args[0]);
                        Assertions.assertEquals("tbl", args[1]);
                        Assertions.assertEquals("p=1", args[2]);
                        Assertions.assertEquals((short) (HmsClientConfig.DEFAULT_PARTITION_BATCH_SIZE + 1), args[3]);
                        return new ArrayList<>(Collections.nCopies(
                                HmsClientConfig.DEFAULT_PARTITION_BATCH_SIZE + 1, new Partition()));
                    }
                    return null;
                });
        ThriftHmsClient client = new ThriftHmsClient(new HmsClientConfig(Collections.emptyMap(), 0),
                new ThriftHmsClient.AuthAction() {
                    @Override
                    public <T> T execute(java.util.concurrent.Callable<T> callable) throws Exception {
                        return callable.call();
                    }
                }, hiveConf -> metastore, HmsTypeMapping.Options.DEFAULT);
        try {
            Assertions.assertThrows(HmsPartitionFilterSaturatedException.class,
                    () -> client.listPartitionsByFilter("db", "tbl", "p=1"));
        } finally {
            client.close();
        }
    }
}
