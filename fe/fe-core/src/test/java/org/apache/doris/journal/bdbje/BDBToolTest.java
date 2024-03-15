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

package org.apache.doris.journal.bdbje;

import org.apache.doris.common.io.DataOutputBuffer;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.ReplicaPersistInfo;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class BDBToolTest {

    private static Environment env;
    private static String path = "./bdb";
    private static Database db;
    private static String dbName = "12345";

    @BeforeClass
    public static void setEnv() {
        try {
            File file = new File("./bdb");
            file.deleteOnExit();
            file.mkdir();

            // init env
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            try {
                env = new Environment(new File(path), envConfig);
            } catch (DatabaseException e) {
                e.printStackTrace();
            }

            // create db
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            try {
                db = env.openDatabase(null, dbName, dbConfig);
            } catch (DatabaseException e) {
                e.printStackTrace();
            }

            // write something
            ReplicaPersistInfo info = ReplicaPersistInfo.createForAdd(1, 2, 3, 4, 5, 6, 7, 8, 0, 10, 11, 0, 12, 14);
            JournalEntity entity = new JournalEntity();
            entity.setOpCode(OperationType.OP_ADD_REPLICA);
            entity.setData(info);

            // id is the key
            Long journalId = 23456L;
            DatabaseEntry theKey = new DatabaseEntry();
            TupleBinding<Long> idBinding = TupleBinding.getPrimitiveBinding(Long.class);
            idBinding.objectToEntry(journalId, theKey);

            // entity is the value
            DataOutputBuffer buffer = new DataOutputBuffer(128);
            try {
                entity.write(buffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            DatabaseEntry theData = new DatabaseEntry(buffer.getData());
            if (db.put(null, theKey, theData) == OperationStatus.SUCCESS) {
                System.out.println("successfully writing the key: " + journalId);
            }

            try {
                if (db != null) {
                    db.close();
                }
                if (env != null) {
                    env.cleanLog();
                    env.close();
                }
            } catch (DatabaseException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void deleteEnv() {
        File file = new File(path);
        if (file.isDirectory()) {
            String[] fileNames = file.list();
            for (int i = 0; i < fileNames.length; i++) {
                File file2 = new File(path + "/" + fileNames[i]);
                file2.delete();
            }
        }
        file.delete();
        System.out.println("file is deleted");
    }

    @Test
    public void testList() {
        BDBToolOptions options = new BDBToolOptions(true, "", false, "", "", 0);
        BDBTool tool = new BDBTool(path, options);
        Assert.assertTrue(tool.run());
    }

    @Test
    public void testDbStat() {
        // wrong db name
        BDBToolOptions options = new BDBToolOptions(false, "12346", true, "", "", 0);
        BDBTool tool = new BDBTool(path, options);
        Assert.assertFalse(tool.run());

        // right db name
        options = new BDBToolOptions(false, "12345", true, "", "", 0);
        tool = new BDBTool(path, options);
        Assert.assertTrue(tool.run());
    }

    @Test
    public void testGetKey() {
        BDBToolOptions options = new BDBToolOptions(false, "12345", false, "", "", 0);
        BDBTool tool = new BDBTool(path, options);
        Assert.assertTrue(tool.run());

        options = new BDBToolOptions(false, "12345", false, "23456", "12345", 0);
        tool = new BDBTool(path, options);
        Assert.assertFalse(tool.run());

        options = new BDBToolOptions(false, "12345", false, "23456", "", 0);
        tool = new BDBTool(path, options);
        Assert.assertTrue(tool.run());
    }

}
