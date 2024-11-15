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

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class ReplaceTableOperationLogTest {
    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./ReplaceTableOperationLogTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        ReplaceTableOperationLog log = new ReplaceTableOperationLog(1, 2, 3, true, true);
        log.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        ReplaceTableOperationLog readLog = ReplaceTableOperationLog.read(dis);
        Assert.assertTrue(readLog.getDbId() == log.getDbId());
        Assert.assertTrue(readLog.getNewTblId() == log.getNewTblId());
        Assert.assertTrue(readLog.getOrigTblId() == log.getOrigTblId());
        Assert.assertTrue(readLog.isSwapTable() == log.isSwapTable());

        // 3. delete files
        dis.close();
        file.delete();
    }
}
