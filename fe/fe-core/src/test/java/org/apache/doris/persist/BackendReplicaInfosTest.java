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

package org.apache.doris.persist;

import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.meta.MetaContext;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class BackendReplicaInfosTest {

    long beId = 1000;
    long tabletId1 = 2001;
    long tabletId2 = 2002;

    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        final Path path = Files.createFile(Paths.get("./BackendReplicaInfosTest"));
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        BackendReplicasInfo info = new BackendReplicasInfo(beId);
        info.addBadReplica(tabletId1);
        info.addMissingVersionReplica(tabletId2, 11);
        checkInfo(info);
        info.write(dos);
        dos.flush();
        dos.close();
        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));

        BackendReplicasInfo rInfo1 = BackendReplicasInfo.read(dis);
        checkInfo(rInfo1);

        // 3. delete files
        dis.close();
        Files.deleteIfExists(path);
    }

    private void checkInfo(BackendReplicasInfo info) {
        Assert.assertFalse(info.isEmpty());
        List<BackendReplicasInfo.ReplicaReportInfo> infos = info.getReplicaReportInfos();
        for (BackendReplicasInfo.ReplicaReportInfo reportInfo : infos) {
            if (reportInfo.tabletId == tabletId1) {
                Assert.assertEquals(BackendReplicasInfo.ReportInfoType.BAD, reportInfo.type);
            } else if (reportInfo.tabletId == tabletId2) {
                Assert.assertEquals(BackendReplicasInfo.ReportInfoType.MISSING_VERSION, reportInfo.type);
                Assert.assertEquals(11, reportInfo.lastFailedVersion);
            } else {
                Assert.fail("unknown tablet id: " + reportInfo.tabletId);
            }
        }
    }
}
