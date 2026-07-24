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

import org.apache.doris.catalog.Replica;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CloudTabletTest {

    @Test
    public void testClearReplicasRemovesCloudReplica() {
        CloudTablet tablet = new CloudTablet(10001L);
        CloudReplica replica = new CloudReplica(20001L, -1L, Replica.ReplicaState.NORMAL, 1L, 0,
                30001L, 40001L, 50001L, 60001L, 0L);

        tablet.addReplica(replica, true);
        Assertions.assertSame(replica, tablet.getCloudReplica());
        Assertions.assertEquals(1, tablet.getReplicas().size());

        tablet.clearReplicas();

        Assertions.assertNull(tablet.getCloudReplica());
        Assertions.assertTrue(tablet.getReplicas().isEmpty());
        Assertions.assertNull(tablet.getReplicaById(replica.getId()));
    }
}
