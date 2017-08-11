// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.journal.bdbje;

import com.baidu.palo.journal.JournalCursor;
import com.baidu.palo.journal.JournalEntity;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.List;

public class BDBJournalCursor implements JournalCursor {
    private static final Logger LOG = LogManager.getLogger(JournalCursor.class);
    
    private long toKey;
    private long currentKey;
    private BDBEnvironment environment;
    private List<Long> dbNames;
    private Database database;
    private int nextDbPositionIndex;
    private final int maxTryTime = 3;
    
    public static BDBJournalCursor getJournalCursor(BDBEnvironment env, long fromKey, long toKey) {
        if (toKey < fromKey || fromKey < 0) {
            System.out.println("Invalid key range!");
            return null;
        }
        BDBJournalCursor cursor = null;
        try {
            cursor = new BDBJournalCursor(env, fromKey, toKey);
        } catch (Exception e) {
            LOG.error("new BDBJournalCursor error.", e);
        }
        return cursor;
    }


    private BDBJournalCursor(BDBEnvironment env, long fromKey, long toKey) throws Exception {
        this.environment = env;
        this.toKey = toKey;
        this.currentKey = fromKey;
        this.dbNames = env.getDatabaseNames();
        if (dbNames == null) {
            throw new NullPointerException("dbNames is null.");
        }
        this.nextDbPositionIndex = 0;
        
        // find the db which may contain the fromKey
        String dbName = null;
        for (long db : dbNames) {
            if (fromKey >= db) {
                dbName = Long.toString(db);
                nextDbPositionIndex++;
                continue;
            } else {
                break;
            }
        }
        
        if (dbName == null) {
            LOG.error("Can not find the key:{}, fail to get journal cursor. will exit.", fromKey);
            System.exit(-1);
        }
        this.database = env.openDatabase(dbName);
    }
    
    @Override
    public JournalEntity next() {
        JournalEntity ret = null;
        if (currentKey > toKey) {
            return ret;
        }
        Long key = new Long(currentKey);
        DatabaseEntry theKey = new DatabaseEntry();
        TupleBinding<Long> myBinding = TupleBinding.getPrimitiveBinding(Long.class);
        myBinding.objectToEntry(key, theKey);
        
        DatabaseEntry theData = new DatabaseEntry();
        // if current db does not contain any more data, then we go to search the next db
        try {
            // null means perform the operation without transaction protection.
            // READ_COMMITTED guarantees no dirty read.
            int tryTimes = 0;
            while (true) {
                if (database.get(null, theKey, theData, LockMode.READ_COMMITTED) == OperationStatus.SUCCESS) {
                    // Recreate the data String.
                    byte[] retData = theData.getData();
                    DataInputStream in = new DataInputStream(new ByteArrayInputStream(retData));
                    ret = new JournalEntity();
                    try {
                        ret.readFields(in);
                    } catch (Exception e) {
                        LOG.error("fail to read journal entity key={}, will exit", currentKey, e);
                        System.exit(-1);
                    }
                    currentKey++;
                    return ret;
                } else if (nextDbPositionIndex < dbNames.size() && currentKey == dbNames.get(nextDbPositionIndex)) {
                    database = environment.openDatabase(dbNames.get(nextDbPositionIndex).toString());
                    nextDbPositionIndex++;
                    tryTimes = 0;
                    continue;
                } else if (tryTimes < maxTryTime) {
                    tryTimes++;
                    LOG.warn("fail to get journal {}, will try again", currentKey);
                    Thread.sleep(3000);
                    continue;
                } else {
                    LOG.error("fail to get journal {}, will exit", currentKey);
                    System.exit(-1);
                }
            }
        } catch (Exception e) {
            LOG.warn("Catch an exception when get next JournalEntity. key:{}", currentKey, e);
            return null;
        }
    }

    @Override
    public void close() {
        
    }
}
