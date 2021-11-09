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

import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.meta.MetaContext;

import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class ColocateTableIndexTest {

    @Test
    public void testGroupId() {
        GroupId groupId1 = new GroupId(1000, 2000);
        GroupId groupId2 = new GroupId(1000, 2000);
        Map<GroupId, Long> map = Maps.newHashMap();
        Assert.assertTrue(groupId1.equals(groupId2));
        Assert.assertTrue(groupId1.hashCode() == groupId2.hashCode());
        map.put(groupId1, 1000L);
        Assert.assertTrue(map.containsKey(groupId2));

        Set<GroupId> balancingGroups = new CopyOnWriteArraySet<GroupId>();
        balancingGroups.add(groupId1);
        Assert.assertTrue(balancingGroups.size() == 1);
        balancingGroups.remove(groupId2);
        Assert.assertTrue(balancingGroups.isEmpty());
    }

    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File("./GroupIdTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        ColocateTableIndex.GroupId groupId = new ColocateTableIndex.GroupId(1, 2);
        groupId.write(dos);
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        ColocateTableIndex.GroupId rGroupId = ColocateTableIndex.GroupId.read(dis);
        Assert.assertTrue(groupId.equals(rGroupId));

        // 3. delete files
        dis.close();
        file.delete();
    }
}
