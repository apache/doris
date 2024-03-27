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

import org.apache.doris.analysis.PasswordOptions;
import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.Privilege;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class PrivInfoTest {

    @Before
    public void setUp() {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();
    }

    @Test
    public void test() throws IOException {
        PrivInfo privInfo = new PrivInfo(UserIdentity.ROOT, PrivBitSet.of(Privilege.ADMIN_PRIV),
                new byte[] {'a', 'b', 'c'}, "role", PasswordOptions.UNSET_OPTION, null, "");

        // 1. Write objects to file
        File file = new File("./privInfo");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        privInfo.write(dos);
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        PrivInfo anotherPrivInfo = PrivInfo.read(dis);
        Assert.assertTrue(Arrays.equals(privInfo.getPasswd(), anotherPrivInfo.getPasswd()));
        Assert.assertEquals(privInfo.getPasswordOptions().getExpirePolicySecond(), anotherPrivInfo.getPasswordOptions()
                .getExpirePolicySecond());
        // 3. delete files
        dis.close();
        file.delete();
    }

    @Test
    public void testWithTablePattern() throws IOException {
        PrivInfo privInfo = new PrivInfo(UserIdentity.ROOT, TablePattern.ALL, PrivBitSet.of(Privilege.DROP_PRIV),
                new byte[] {'a', 'b', 'c'}, "role", null);

        // 1. Write objects to file
        File file = new File("./privInfo");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        privInfo.write(dos);
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        PrivInfo anotherPrivInfo = PrivInfo.read(dis);
        Assert.assertTrue(Arrays.equals(privInfo.getPasswd(), anotherPrivInfo.getPasswd()));
        Assert.assertEquals(PasswordOptions.UNSET, anotherPrivInfo.getPasswordOptions().getExpirePolicySecond());
        Assert.assertEquals(privInfo.getTblPattern().getTbl(), anotherPrivInfo.getTblPattern().getTbl());

        // 3. delete files
        dis.close();
        file.delete();
    }

    @Test
    public void testWithResourcePattern() throws IOException {
        PrivInfo privInfo = new PrivInfo(UserIdentity.ROOT, new ResourcePattern("res1", ResourceTypeEnum.GENERAL),
                PrivBitSet.of(Privilege.DROP_PRIV), new byte[] {'a', 'b', 'c'}, "role");

        // 1. Write objects to file
        File file = new File("./privInfo");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        privInfo.write(dos);
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        PrivInfo anotherPrivInfo = PrivInfo.read(dis);
        Assert.assertTrue(Arrays.equals(privInfo.getPasswd(), anotherPrivInfo.getPasswd()));
        Assert.assertEquals(PasswordOptions.UNSET, anotherPrivInfo.getPasswordOptions().getExpirePolicySecond());
        Assert.assertEquals(privInfo.getResourcePattern(), anotherPrivInfo.getResourcePattern());

        // 3. delete files
        dis.close();
        file.delete();
    }
}
