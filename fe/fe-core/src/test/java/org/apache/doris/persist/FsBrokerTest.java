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

import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.system.BrokerHbResponse;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FsBrokerTest {

    private static final String fileName1 = "./FsBrokerTest1";
    private static final String fileName2 = "./FsBrokerTest2";

    @BeforeClass
    public static void setup() {
        MetaContext context = new MetaContext();
        context.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        context.setThreadLocalInfo();
    }

    @AfterClass
    public static void tear() throws IOException {
        Files.deleteIfExists(Paths.get(fileName1));
        Files.deleteIfExists(Paths.get(fileName2));
    }

    @Test
    public void testHeartbeatOk() throws Exception {
        // 1. Write objects to file
        final Path path = Files.createFile(Paths.get(fileName1));
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        FsBroker fsBroker = new FsBroker("127.0.0.1", 8118);
        long time = System.currentTimeMillis();
        BrokerHbResponse hbResponse = new BrokerHbResponse("broker", "127.0.0.1", 8118, time);
        fsBroker.handleHbResponse(hbResponse);
        fsBroker.write(dos);
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));

        FsBroker readBroker = FsBroker.readIn(dis);
        Assert.assertEquals(fsBroker.ip, readBroker.ip);
        Assert.assertEquals(fsBroker.port, readBroker.port);
        Assert.assertEquals(fsBroker.isAlive, readBroker.isAlive);
        Assert.assertTrue(fsBroker.isAlive);
        Assert.assertEquals(time, readBroker.lastStartTime);
        Assert.assertEquals(-1, readBroker.lastUpdateTime);
        dis.close();
    }

    @Test
    public void testHeartbeatFailed() throws Exception {
        // 1. Write objects to file
        final Path path = Files.createFile(Paths.get(fileName2));
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        FsBroker fsBroker = new FsBroker("127.0.0.1", 8118);
        BrokerHbResponse hbResponse = new BrokerHbResponse("broker", "127.0.0.1", 8118, "got exception");
        fsBroker.handleHbResponse(hbResponse);
        fsBroker.write(dos);
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));

        FsBroker readBroker = FsBroker.readIn(dis);
        Assert.assertEquals(fsBroker.ip, readBroker.ip);
        Assert.assertEquals(fsBroker.port, readBroker.port);
        Assert.assertEquals(fsBroker.isAlive, readBroker.isAlive);
        Assert.assertFalse(fsBroker.isAlive);
        Assert.assertEquals(-1, readBroker.lastStartTime);
        Assert.assertEquals(-1, readBroker.lastUpdateTime);
        dis.close();
    }
}
