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

package org.apache.doris.catalog;

import org.apache.doris.catalog.ReplicaAllocation.AllocationType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.resource.Tag;
import org.apache.doris.resource.TagSet;
import org.apache.doris.system.Backend;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/*
 * Author: Chenmingyu
 * Date: Dec 20, 2019
 */

public class ReplicaAllocationTest {

    @Test
    public void testDefault() {
        ReplicaAllocation replicaAllocation = ReplicaAllocation.createDefault((short) 2, "default_cluster");
        Assert.assertEquals(2, replicaAllocation.getReplicaNum());
        Assert.assertEquals(2, replicaAllocation.getReplicaNumByType(AllocationType.LOCAL));
        Map<TagSet, Short> tagMap = replicaAllocation.getTagMapByType(AllocationType.REMOTE);
        Assert.assertTrue(tagMap.isEmpty());
        tagMap = replicaAllocation.getTagMapByType(AllocationType.LOCAL);
        Assert.assertTrue(tagMap.containsKey(Backend.DEFAULT_TAG_SET));
        Assert.assertEquals(2, (int) tagMap.get(Backend.DEFAULT_TAG_SET));
    }

    @Test
    public void testCustom() throws AnalysisException {
        ReplicaAllocation replicaAllocation = new ReplicaAllocation();
        TagSet tagSet = TagSet.create(Tag.create(Tag.TYPE_FUNCTION, Tag.VALUE_COMPUTATION),
                Tag.create(Tag.TYPE_FUNCTION, Tag.VALUE_STORE),
                Tag.create("custom", "tag1"));
        replicaAllocation.setReplica(AllocationType.LOCAL, tagSet, (short) 3);

        Assert.assertEquals(3, replicaAllocation.getReplicaNum());
        Assert.assertEquals(3, replicaAllocation.getReplicaNumByType(AllocationType.LOCAL));
        Map<TagSet, Short> tagMap = replicaAllocation.getTagMapByType(AllocationType.REMOTE);
        Assert.assertTrue(tagMap.isEmpty());
        tagMap = replicaAllocation.getTagMapByType(AllocationType.LOCAL);
        Assert.assertTrue(tagMap.containsKey(tagSet));
        Assert.assertEquals(3, (int) tagMap.get(tagSet));
    }
}
