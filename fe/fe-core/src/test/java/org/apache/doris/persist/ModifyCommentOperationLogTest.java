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

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Map;

public class ModifyCommentOperationLogTest {
    @Test
    public void testColCommentSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./ModifyColumnCommentOperationLogTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        Map<String, String> colToComment = Maps.newHashMap();
        colToComment.put("k1", "comment1");
        colToComment.put("k2", "comment2");
        ModifyCommentOperationLog log = ModifyCommentOperationLog.forColumn(1L, 2L, colToComment);
        log.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        ModifyCommentOperationLog readLog = ModifyCommentOperationLog.read(dis);
        Assert.assertTrue(readLog.getType() == ModifyCommentOperationLog.Type.COLUMN);
        Assert.assertTrue(readLog.getDbId() == log.getDbId());
        Assert.assertTrue(readLog.getTblId() == log.getTblId());
        Assert.assertTrue(readLog.getTblComment() == null);
        Assert.assertTrue(readLog.getColToComment().size() == 2);

        // 3. delete files
        dis.close();
        file.delete();
    }

    @Test
    public void testTableCommentSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./ModifyTableCommentOperationLogTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        ModifyCommentOperationLog log = ModifyCommentOperationLog.forTable(1L, 2L, "comment");
        log.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        ModifyCommentOperationLog readLog = ModifyCommentOperationLog.read(dis);
        Assert.assertTrue(readLog.getType() == ModifyCommentOperationLog.Type.TABLE);
        Assert.assertTrue(readLog.getDbId() == log.getDbId());
        Assert.assertTrue(readLog.getTblId() == log.getTblId());
        Assert.assertEquals("comment", readLog.getTblComment());
        Assert.assertTrue(readLog.getColToComment() == null);

        // 3. delete files
        dis.close();
        file.delete();
    }
}
