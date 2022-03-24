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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.DataOutputBuffer;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.Util;
import org.apache.doris.journal.Journal;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.OperationType;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.RollbackException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/* 
 * This is the bdb implementation of Journal interface.
 * First, we open() this journal, then read from or write to the bdb environment
 * We can also get journal id information by calling get***Id functions.
 * Finally, close this journal.
 * This class encapsulates the read, write APIs of bdbje
 */
public class BDBJEJournal implements Journal {
    public static final Logger LOG = LogManager.getLogger(BDBJEJournal.class);
    private static final int OUTPUT_BUFFER_INIT_SIZE = 128;
    private static final int RETRY_TIME = 3;
    
    private String environmentPath = null;
    private String selfNodeName;
    private String selfNodeHostPort;
    
    private BDBEnvironment bdbEnvironment = null;
    private Database currentJournalDB;
    // the next journal's id. start from 1.
    private AtomicLong nextJournalId = new AtomicLong(1);
    
    public BDBJEJournal(String nodeName) {
        initBDBEnv(nodeName);
    }
    
    /* 
     * Initialize bdb environment.
     * node name is ip_port (the port is edit_log_port)
     */
    private void initBDBEnv(String nodeName) {
        environmentPath = Catalog.getServingCatalog().getBdbDir();
        Pair<String, Integer> selfNode = Catalog.getServingCatalog().getSelfNode();
        selfNodeName = nodeName;
        selfNodeHostPort = selfNode.first + ":" + selfNode.second;
    }

    /*
     * Database is named by its minimum journal id.
     * For example:
     * One database contains journal 100 to journal 200, its name is 100.
     * The next database's name is 201
     */
    @Override
    public synchronized void rollJournal() {
        // Doesn't need to roll if current database contains no journals
        if (currentJournalDB.count() == 0) {
            return;
        }
        
        long newName = nextJournalId.get();
        String currentDbName = currentJournalDB.getDatabaseName();
        long currentName = Long.parseLong(currentDbName);
        long newNameVerify = currentName + currentJournalDB.count();
        if (newName == newNameVerify) {
            LOG.info("roll edit log. new db name is {}", newName);
            currentJournalDB = bdbEnvironment.openDatabase(Long.toString(newName));
        } else {
            String msg = String.format("roll journal error! journalId and db journal numbers is not match. "
                    + "journal id: %d, current db: %s, expected db count: %d",
                    newName, currentDbName, newNameVerify);
            LOG.error(msg);
            Util.stdoutWithTime(msg);
            System.exit(-1);
        }
    }

    @Override
    public synchronized void write(short op, Writable writable) throws IOException {
        JournalEntity entity = new JournalEntity();
        entity.setOpCode(op);
        entity.setData(writable);

        // id is the key
        long id = nextJournalId.getAndIncrement();
        Long idLong = id;
        DatabaseEntry theKey = new DatabaseEntry();
        TupleBinding<Long> idBinding = TupleBinding.getPrimitiveBinding(Long.class);
        idBinding.objectToEntry(idLong, theKey);

        // entity is the value
        DataOutputBuffer buffer = new DataOutputBuffer(OUTPUT_BUFFER_INIT_SIZE);
        entity.write(buffer);

        DatabaseEntry theData = new DatabaseEntry(buffer.getData());
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_EDIT_LOG_SIZE_BYTES.increase((long) theData.getSize());
        }
        LOG.debug("opCode = {}, journal size = {}", op, theData.getSize());
        // Write the key value pair to bdb.
        boolean writeSucceed = false;
        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                // Parameter null means auto commit
                if (currentJournalDB.put(null, theKey, theData) == OperationStatus.SUCCESS) {
                    writeSucceed = true;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("master write journal {} finished. db name {}, current time {}",
                                id, currentJournalDB.getDatabaseName(), System.currentTimeMillis());
                    }
                    break;
                }
            } catch (DatabaseException e) {
                LOG.error("catch an exception when writing to database. sleep and retry. journal id {}", id, e);
                try {
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }

        if (!writeSucceed) {
            if (op == OperationType.OP_TIMESTAMP) {
                /*
                 * Do not exit if the write operation is OP_TIMESTAMP.
                 * If all the followers exit except master, master should continue provide query service.
                 * To prevent master exit, we should exempt OP_TIMESTAMP write
                 */
                nextJournalId.set(id);
                LOG.warn("master can not achieve quorum. write timestamp fail. but will not exit.");
                return;
            }
            String msg = "write bdb failed. will exit. journalId: " + id + ", bdb database Name: " +
                    currentJournalDB.getDatabaseName();
            LOG.error(msg);
            Util.stdoutWithTime(msg);
            System.exit(-1);
        }
    }

    @Override
    public JournalEntity read(long journalId) {
        List<Long> dbNames = getDatabaseNames();
        if (dbNames == null) {
            return null;
        }
        String dbName = null;
        for (long db : dbNames) {
            if (journalId >= db) {
                dbName = Long.toString(db);
                continue;
            } else {
                break;
            }
        }
        
        if (dbName == null) {
            return null;
        }
        
        JournalEntity ret = null;
        Long key = new Long(journalId);
        DatabaseEntry theKey = new DatabaseEntry();
        TupleBinding<Long> myBinding = TupleBinding.getPrimitiveBinding(Long.class);
        myBinding.objectToEntry(key, theKey);
        
        DatabaseEntry theData = new DatabaseEntry();
        
        Database database = bdbEnvironment.openDatabase(dbName);
        try {
            // null means perform the operation without transaction protection.
            // READ_COMMITTED guarantees no dirty read.
            if (database.get(null, theKey, theData, LockMode.READ_COMMITTED) == OperationStatus.SUCCESS) {
                // Recreate the data String.
                byte[] retData = theData.getData();
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(retData));
                ret = new JournalEntity();
                try {
                    ret.readFields(in);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("No record found for key '" + journalId + "'.");
            }
        } catch (Exception e) {
            LOG.warn("catch an exception when get JournalEntity. key:{}", journalId, e);
            return null;
        }
        return ret;
    }

    @Override
    public JournalCursor read(long fromKey, long toKey) {
        return BDBJournalCursor.getJournalCursor(bdbEnvironment, fromKey, toKey);
    }
    
    @Override
    public long getMaxJournalId() {
        long ret = -1;
        if (bdbEnvironment == null) {
            return ret;
        }
        List<Long> dbNames = getDatabaseNames();
        if (dbNames == null) {
            return ret;
        }
        if (dbNames.size() == 0) {
            return ret;
        }
        
        int index = dbNames.size() - 1;
        String dbName = dbNames.get(index).toString();
        long dbNumberName = dbNames.get(index);
        Database database = bdbEnvironment.openDatabase(dbName);
        ret = dbNumberName + database.count() - 1;
        
        return ret;
    }

    @Override
    public long getMinJournalId() {
        long ret = -1;
        if (bdbEnvironment == null) {
            return ret;
        }
        List<Long> dbNames = getDatabaseNames();
        if (dbNames == null) {
            return ret;
        }
        if (dbNames.size() == 0) {
            return ret;
        }
        
        String dbName = dbNames.get(0).toString();
        Database database = bdbEnvironment.openDatabase(dbName);
        // The database is empty
        if (database.count() == 0) {
            return ret;
        }
        
        return dbNames.get(0);
    }

    @Override
    public void close() {
        bdbEnvironment.close();
        bdbEnvironment = null;
    }

    /*
     * open the bdbje environment, and get the current journal database
     */
    @Override
    public synchronized void open() {
        if (bdbEnvironment == null) {
            File dbEnv = new File(environmentPath);
            bdbEnvironment = new BDBEnvironment();
            Pair<String, Integer> helperNode = Catalog.getServingCatalog().getHelperNode();
            String helperHostPort = helperNode.first + ":" + helperNode.second;
            try {
                bdbEnvironment.setup(dbEnv, selfNodeName, selfNodeHostPort,
                        helperHostPort, Catalog.getServingCatalog().isElectable());
            } catch (Exception e) {
                LOG.error("catch an exception when setup bdb environment. will exit.", e);
                System.exit(-1);
            }
        }
        
        // Open a new journal database or get last existing one as current journal database
        List<Long> dbNames = null;
        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                dbNames = getDatabaseNames();
                
                if (dbNames == null) {
                    LOG.error("fail to get dbNames while open bdbje journal. will exit");
                    System.exit(-1);
                }
                if (dbNames.size() == 0) {
                    /*
                     *  This is the very first time to open. Usually, we will open a new database named "1".
                     *  But when we start cluster with an image file copied from other cluster,
                     *  here we should open database with name image max journal id + 1.
                     *  (default Catalog.getServingCatalog().getReplayedJournalId() is 0)
                     */
                    String dbName = Long.toString(Catalog.getServingCatalog().getReplayedJournalId() + 1);
                    LOG.info("the very first time to open bdb, dbname is {}", dbName);
                    currentJournalDB = bdbEnvironment.openDatabase(dbName);
                } else {
                    // get last database as current journal database
                    currentJournalDB = bdbEnvironment.openDatabase(dbNames.get(dbNames.size() - 1).toString());
                }

                // set next journal id
                nextJournalId.set(getMaxJournalId() + 1);

                break;
            } catch (InsufficientLogException insufficientLogEx) {
                reSetupBdbEnvironment(insufficientLogEx);
            }
        }
    }

    private void reSetupBdbEnvironment(InsufficientLogException insufficientLogEx) {
        LOG.warn("catch insufficient log exception. will recover and try again.", insufficientLogEx);
        // Copy the missing log files from a member of the replication group who owns the files
        // ATTN: here we use `getServingCatalog()`, because only serving catalog has helper nodes.
        Pair<String, Integer> helperNode = Catalog.getServingCatalog().getHelperNode();
        NetworkRestore restore = new NetworkRestore();
        NetworkRestoreConfig config = new NetworkRestoreConfig();
        config.setRetainLogFiles(false);
        restore.execute(insufficientLogEx, config);
        bdbEnvironment.close();
        bdbEnvironment.setup(new File(environmentPath), selfNodeName, selfNodeHostPort,
                helperNode.first + ":" + helperNode.second,
                Catalog.getServingCatalog().isElectable());
    }

    @Override
    public void deleteJournals(long deleteToJournalId) {
        List<Long> dbNames = getDatabaseNames();
        if (dbNames == null) {
            LOG.info("delete database names is null.");
            return;
        }

        String msg = "existing database names: ";
        for (long name : dbNames) {
            msg += name + " ";
        }
        msg += ", deleteToJournalId is " + deleteToJournalId;
        LOG.info(msg);
        
        for (int i = 1; i < dbNames.size(); i++) {
            if (deleteToJournalId >= dbNames.get(i)) {
                long name = dbNames.get(i - 1);
                String stringName = Long.toString(name);
                LOG.info("delete database name {}", stringName);
                bdbEnvironment.removeDatabase(stringName);
            } else {
                LOG.info("database name {} is larger than deleteToJournalId {}, not delete",
                        dbNames.get(i), deleteToJournalId);
                break;
            }
        }
    }
    
    @Override
    public long getFinalizedJournalId() {
        List<Long> dbNames = getDatabaseNames();
        if (dbNames == null) {
            LOG.error("database name is null.");
            return 0;
        }
        
        String msg = "database names: ";
        for (long name : dbNames) {
            msg += name + " ";
        }
        LOG.info(msg);
        
        if (dbNames.size() < 2) {
            return 0;
        }
        
        return dbNames.get(dbNames.size() - 1) - 1;
    }
    
    @Override
    public List<Long> getDatabaseNames() {
        if (bdbEnvironment == null) {
            return null;
        }

        // Open a new journal database or get last existing one as current journal database
        List<Long>  dbNames = null;
        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                dbNames = bdbEnvironment.getDatabaseNames();
                break;
            } catch (InsufficientLogException insufficientLogEx) {
                /*
                 * If this is not a checkpoint thread, which means this maybe the FE startup thread,
                 * or a replay thread. We will reopen bdbEnvironment for these 2 cases to get valid log
                 * from helper nodes.
                 *
                 * The checkpoint thread will only run on Master FE. And Master FE should not encounter
                 * these exception. So if it happens, throw exception out.
                 */
                if (!Catalog.isCheckpointThread()) {
                    reSetupBdbEnvironment(insufficientLogEx);
                } else {
                    throw insufficientLogEx;
                }
            } catch (RollbackException rollbackEx) {
                if (!Catalog.isCheckpointThread()) {
                    LOG.warn("catch rollback log exception. will reopen the ReplicatedEnvironment.", rollbackEx);
                    bdbEnvironment.closeReplicatedEnvironment();
                    bdbEnvironment.openReplicatedEnvironment(new File(environmentPath));
                } else {
                    throw rollbackEx;
                }
            }
        }

        return dbNames;
    }
}
