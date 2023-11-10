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

import org.apache.doris.common.Pair;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;

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
    private static final Logger LOG = LogManager.getLogger(BDBJournalCursor.class);

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
    public Pair<Long, JournalEntity> next() {
        if (currentKey > toKey) {
            return null;
        }

        Long key = currentKey;
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
                OperationStatus operationStatus = database.get(null, theKey, theData, LockMode.READ_COMMITTED);
                if (operationStatus == OperationStatus.SUCCESS) {
                    // Recreate the data String.
                    byte[] retData = theData.getData();
                    DataInputStream in = new DataInputStream(new ByteArrayInputStream(retData));
                    JournalEntity entity = new JournalEntity();
                    try {
                        entity.readFields(in);
                        entity.setDataSize(retData.length);
                    } catch (Exception e) {
                        LOG.error("fail to read journal entity key={}, will exit", currentKey, e);
                        System.exit(-1);
                    }
                    currentKey++;
                    return Pair.of(key, entity);
                } else if (nextDbPositionIndex < dbNames.size() && currentKey == dbNames.get(nextDbPositionIndex)) {
                    database = environment.openDatabase(dbNames.get(nextDbPositionIndex).toString());
                    nextDbPositionIndex++;
                    tryTimes = 0;
                } else if (tryTimes < maxTryTime) {
                    tryTimes++;
                    LOG.warn("fail to get journal {}, will try again. status: {}", currentKey, operationStatus);
                    Thread.sleep(3000);
                } else if (operationStatus == OperationStatus.NOTFOUND) {
                    // In the case:
                    // On non-master FE, the replayer will first get the max journal id,
                    // than try to replay logs from current replayed id to the max journal id. But when
                    // master FE try to write a log to bdbje, but crashed before this log is committed,
                    // the non-master FE may still get this incomplete log's id as max journal id,
                    // and try to replay it. We will first get LockTimeoutException (because the transaction
                    // is hanging and waiting to be aborted after timeout). and after this log abort,
                    // we will get NOTFOUND.
                    // So we simply throw a exception and let the replayer get the max id again.
                    throw new Exception(
                            "Failed to find key " + currentKey + " in database " + database.getDatabaseName());
                } else {
                    LOG.error("fail to get journal {}, status: {}, will exit", currentKey, operationStatus);
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
