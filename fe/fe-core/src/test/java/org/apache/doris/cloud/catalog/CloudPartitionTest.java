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

package org.apache.doris.cloud.catalog;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.VersionHelper;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.rpc.RpcException;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CloudPartitionTest {

    @Ignore
    public void getCachedVisibleVersion() {
    }

    public static CloudPartition createPartition(long pId, long dbId, long tblId) {
        return new CloudPartition(pId, "p" + pId, null, null, dbId, tblId);
    }

    @Test
    public void testIsCachedVersionExpired() {
        // test isCachedVersionExpired
        CloudPartition part = createPartition(1, 2, 3);
        SessionVariable.cloudPartitionVersionCacheTtlMs = 0;
        Assertions.assertTrue(part.isCachedVersionExpired());
        SessionVariable.cloudPartitionVersionCacheTtlMs = -10086;
        part.setCachedVisibleVersion(2, 10086L); // update version and last cache time
        SessionVariable.cloudPartitionVersionCacheTtlMs = 10000;
        Assertions.assertFalse(part.isCachedVersionExpired()); // not expired due to long expiration duration
        Assertions.assertEquals(2, part.getCachedVisibleVersion());

    }

    @Test
    public void testCachedVersion() throws RpcException {
        CloudPartition part = createPartition(1, 2, 3);
        List<CloudPartition> parts = new ArrayList<>();
        for (long i = 0; i < 3; ++i) {
            parts.add(createPartition(5 + i, 5 + i, 5 + i));
        }
        Assertions.assertEquals(-1, part.getCachedVisibleVersion()); // not initialized FE side version

        // CHECKSTYLE OFF
        final ArrayList<Long> singleVersions = new ArrayList<>(Arrays.asList(2L, 3L, 4L, 5L));
        final ArrayList<ArrayList<Long>> batchVersions = new ArrayList<>(Arrays.asList(
                new ArrayList<>(Arrays.asList(1L, 2L, -1L)),
                new ArrayList<>(Arrays.asList(2L, 3L, -1L)),
                new ArrayList<>(Arrays.asList(3L, 4L, -1L)),
                new ArrayList<>(Arrays.asList(5L, -1L))
        ));
        final Integer[] callCount = {0};

        new MockUp<VersionHelper>(VersionHelper.class) {
            @Mock
            public Cloud.GetVersionResponse getVersionFromMeta(Cloud.GetVersionRequest req) {
                Cloud.GetVersionResponse.Builder builder = Cloud.GetVersionResponse.newBuilder();
                builder.setVersion(singleVersions.get(callCount[0]));
                builder.addAllVersions(batchVersions.get(callCount[0]));
                ++callCount[0];
                return builder.build();
            }
        };
        // CHECKSTYLE ON

        SessionVariable.cloudPartitionVersionCacheTtlMs = -1; // disable cache
            {
                // test single get version
                Assertions.assertEquals(2, part.getVisibleVersion()); // should not get from cache
                Assertions.assertEquals(1, callCount[0]); // issue a rpc call to meta-service

                // test snapshot versions
                List<Long> versions = CloudPartition.getSnapshotVisibleVersion(parts); // should not get from cache
                Assertions.assertEquals(2, callCount[0]); // issue a rpc call to meta-service
                for (int i = 0; i < batchVersions.get(1).size(); ++i) {
                    Long exp = batchVersions.get(1).get(i);
                    if (exp == -1) {
                        exp = CloudPartition.PARTITION_INIT_VERSION;
                    }
                    Assertions.assertEquals(exp, versions.get(i));
                }
            }

        // enable change expiration and make it cached in long duration
        SessionVariable.cloudPartitionVersionCacheTtlMs = 100000;
            {
                // test single get version
                Assertions.assertEquals(2, part.getVisibleVersion()); // cached version
                Assertions.assertEquals(2, callCount[0]); // issue a rpc call to meta-service

                // test snapshot versions
                List<Long> versions = CloudPartition.getSnapshotVisibleVersion(parts); // should not get from cache
                Assertions.assertEquals(2, callCount[0]); // issue a rpc call to meta-service
                for (int i = 0; i < batchVersions.get(1).size(); ++i) {
                    Long exp = batchVersions.get(1).get(i);
                    if (exp == -1) {
                        exp = CloudPartition.PARTITION_INIT_VERSION;
                    }
                    Assertions.assertEquals(exp, versions.get(i));
                }
            }

        // enable change expiration and make it expired
        SessionVariable.cloudPartitionVersionCacheTtlMs = 500;
        try {
            Thread.sleep(550);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        // make some partition not expired, these partitions will not get version from meta-service
        CloudPartition hotPartition = parts.get(0);
        hotPartition.setCachedVisibleVersion(hotPartition.getCachedVisibleVersion(), 10086L);
        Assertions.assertEquals(2, hotPartition.getCachedVisibleVersion());
        Assertions.assertFalse(hotPartition.isCachedVersionExpired());
        Assertions.assertTrue(parts.get(1).isCachedVersionExpired());
        Assertions.assertTrue(parts.get(2).isCachedVersionExpired());

            {
                // test single get version
                Assertions.assertEquals(4, part.getVisibleVersion()); // should not get from cache
                Assertions.assertEquals(3, callCount[0]); // issue a rpc call to meta-service

                // test snapshot versions
                List<Long> versions = CloudPartition.getSnapshotVisibleVersion(parts); // should not get from cache
                Assertions.assertEquals(versions.size(), parts.size());
                Assertions.assertEquals(4, callCount[0]); // issue a rpc call to meta-service
                for (int i = 0; i < batchVersions.get(3).size(); ++i) {
                    Long exp = batchVersions.get(3).get(i);
                    if (exp == -1) {
                        exp = CloudPartition.PARTITION_INIT_VERSION;
                    }
                    Assertions.assertEquals(exp, versions.get(i + 1)); // exclude the first hot partition
                }
                // hot partition version not changed
                Assertions.assertEquals(2, hotPartition.getCachedVisibleVersion());
            }
    }
}
