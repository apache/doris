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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.DataOutputBuffer;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.journal.Journal;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.OperationType;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.TimeConsistencyPolicy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/*
 * This is the bdb implementation of Journal interface.
 * First, we open() this journal, then read from or write to the bdb environment
 * We can also get journal id information by calling get***Id functions.
 * Finally, close this journal.
 * This class encapsulates the read, write APIs of bdbje
 */
public class BDBJEJournal implements Journal { // CHECKSTYLE IGNORE THIS LINE: BDBJE should use uppercase
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
        environmentPath = Env.getServingEnv().getBdbDir();
        HostInfo selfNode = Env.getServingEnv().getSelfNode();
        selfNodeName = nodeName;
        // We use the hostname as the address of the bdbje node,
        // so that we do not need to update bdbje when the IP changes.
        // WARNING:However, it is necessary to ensure that the hostname of the node
        // can be resolved and accessed by other nodes.
        selfNodeHostPort = NetUtils.getHostPortInAccessibleFormat(selfNode.getHost(), selfNode.getPort());
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
            LOG.info("roll edit log. new dbName: {}, old dbName:{}", newName, currentDbName);
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
    public synchronized long write(short op, Writable writable) throws IOException {
        JournalEntity entity = new JournalEntity();
        entity.setOpCode(op);
        entity.setData(writable);

        // id is the key
        long id = nextJournalId.getAndIncrement();
        DatabaseEntry theKey = idToKey(id);

        // entity is the value
        DataOutputBuffer buffer = new DataOutputBuffer(OUTPUT_BUFFER_INIT_SIZE);
        entity.write(buffer);

        DatabaseEntry theData = new DatabaseEntry(buffer.getData());
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_EDIT_LOG_SIZE_BYTES.increase((long) theData.getSize());
            MetricRepo.COUNTER_CURRENT_EDIT_LOG_SIZE_BYTES.increase((long) theData.getSize());
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
            } catch (ReplicaWriteException e) {
                /**
                 * This exception indicates that an update operation or transaction commit
                 * or abort was attempted while in the
                 * {@link ReplicatedEnvironment.State#REPLICA} state. The transaction is marked
                 * as being invalid.
                 * <p>
                 * The exception is the result of either an error in the application logic or
                 * the result of a transition of the node from Master to Replica while a
                 * transaction was in progress.
                 * <p>
                 * The application must abort the current transaction and redirect all
                 * subsequent update operations to the Master.
                 */
                LOG.error("catch ReplicaWriteException when writing to database, will exit. journal id {}", id, e);
                String msg = "write bdb failed. will exit. journalId: " + id + ", bdb database Name: "
                        + currentJournalDB.getDatabaseName();
                LOG.error(msg);
                Util.stdoutWithTime(msg);
                System.exit(-1);
            } catch (DatabaseException e) {
                LOG.error("catch an exception when writing to database. sleep and retry. journal id {}", id, e);
                try {
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e1) {
                    LOG.warn("", e1);
                }
            }
        }

        if (!writeSucceed) {
            if (op == OperationType.OP_TIMESTAMP) {
                /*
                 * Do not exit if the write operation is OP_TIMESTAMP.
                 * If all the followers exit except master, master should continue provide query
                 * service.
                 * To prevent master exit, we should exempt OP_TIMESTAMP write
                 */
                nextJournalId.set(id);
                LOG.warn("master can not achieve quorum. write timestamp fail. but will not exit.");
                return -1;
            }
            String msg = "write bdb failed. will exit. journalId: " + id + ", bdb database Name: "
                    + currentJournalDB.getDatabaseName();
            LOG.error(msg);
            Util.stdoutWithTime(msg);
            System.exit(-1);
        }
        return id;
    }

    private static DatabaseEntry idToKey(Long id) {
        DatabaseEntry theKey = new DatabaseEntry();
        TupleBinding<Long> idBinding = TupleBinding.getPrimitiveBinding(Long.class);
        idBinding.objectToEntry(id, theKey);
        return theKey;
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
        Long key = journalId;
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
                    LOG.warn("", e);
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
        return getMaxJournalIdInternal(true);
    }

    // get max journal id but do not check whether the txn is matched.
    private long getMaxJournalIdWithoutCheck() {
        return getMaxJournalIdInternal(false);
    }

    private long getMaxJournalIdInternal(boolean checkTxnMatched) {
        long ret = -1;
        if (bdbEnvironment == null) {
            return ret;
        }
        List<Long> dbNames = getDatabaseNames();
        if (dbNames == null) {
            return ret;
        }
        if (dbNames.isEmpty()) {
            return ret;
        }

        int index = dbNames.size() - 1;
        String dbName = dbNames.get(index).toString();
        long dbNumberName = dbNames.get(index);
        Database database = bdbEnvironment.openDatabase(dbName);
        if (checkTxnMatched && !isReplicaTxnAreMatched(database, dbNumberName)) {
            LOG.warn("The current replica hasn't synced up with the master, current db name: {}", dbNumberName);
            if (index != 0) {
                // Because roll journal occurs after write, the previous write must have
                // been replicated to the majority, so it can be guaranteed that the database
                // will not be rollback.
                return dbNumberName - 1;
            }
            return -1;
        }
        return dbNumberName + database.count() - 1;
    }

    // Whether the replica txns are matched with the master.
    //
    // BDBJE could throw InsufficientAcksException during post commit, at that time the
    // log has persisted in disk. When the replica is restarted, we need to ensure that
    // before replaying the journals, sync up txns with the new master in the cluster and
    // rollback the txns that have been persisted but have not committed to the majority.
    //
    // See org.apache.doris.journal.bdbje.BDBEnvironmentTest#testReadTxnIsNotMatched for details.
    private boolean isReplicaTxnAreMatched(Database database, Long id) {
        // The time lag is set to Integer.MAX_VALUE if the replica haven't synced up
        // with the master. By allowing a very large lag, we can detect whether the
        // replica has synced up with the master.
        TimeConsistencyPolicy consistencyPolicy = new TimeConsistencyPolicy(
                1, TimeUnit.DAYS, 1, TimeUnit.MINUTES);
        Transaction txn = null;
        try {
            TransactionConfig cfg = new TransactionConfig()
                    .setReadOnly(true)
                    .setReadCommitted(true)
                    .setConsistencyPolicy(consistencyPolicy);

            txn = bdbEnvironment.getReplicatedEnvironment().beginTransaction(null, cfg);

            DatabaseEntry key = idToKey(id);
            database.get(txn, key, null, LockMode.READ_COMMITTED);
            return true;
        } catch (ReplicaConsistencyException e) {
            return false;
        } finally {
            if (txn != null) {
                txn.abort();
            }
        }
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
        if (dbNames.isEmpty()) {
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

            boolean metadataFailureRecovery = null != System.getProperty(FeConstants.METADATA_FAILURE_RECOVERY_KEY);
            bdbEnvironment = new BDBEnvironment(Env.getServingEnv().isElectable(), metadataFailureRecovery);

            HostInfo helperNode = Env.getServingEnv().getHelperNode();
            String helperHostPort = NetUtils.getHostPortInAccessibleFormat(helperNode.getHost(), helperNode.getPort());
            try {
                bdbEnvironment.setup(dbEnv, selfNodeName, selfNodeHostPort, helperHostPort);
            } catch (Exception e) {
                if (e instanceof DatabaseNotFoundException) {
                    LOG.error("It is not allowed to set metadata_failure_recovery"
                            + "when meta dir or bdbje dir is empty， which may mean it is "
                            + "the first time to start this node");
                }
                LOG.error("catch an exception when setup bdb environment. will exit.", e);
                System.exit(-1);
            }
        }

        // Open a new journal database or get last existing one as current journal
        // database
        List<Long> dbNames = null;
        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                dbNames = getDatabaseNames();

                if (dbNames == null) {
                    LOG.error("fail to get dbNames while open bdbje journal. will exit");
                    System.exit(-1);
                }
                if (dbNames.isEmpty()) {
                    /*
                     * This is the very first time to open. Usually, we will open a new database
                     * named "1".
                     * But when we start cluster with an image file copied from other cluster,
                     * here we should open database with name image max journal id + 1.
                     * (default Catalog.getServingEnv().getReplayedJournalId() is 0)
                     */
                    String dbName = Long.toString(Env.getServingEnv().getReplayedJournalId() + 1);
                    LOG.info("the very first time to open bdb, dbname is {}", dbName);
                    currentJournalDB = bdbEnvironment.openDatabase(dbName);
                } else {
                    // get last database as current journal database
                    currentJournalDB = bdbEnvironment.openDatabase(dbNames.get(dbNames.size() - 1).toString());
                }

                // set next journal id
                nextJournalId.set(getMaxJournalIdWithoutCheck() + 1);

                break;
            } catch (InsufficientLogException insufficientLogEx) {
                reSetupBdbEnvironment(insufficientLogEx);
            } catch (RollbackException rollbackEx) {
                LOG.warn("catch rollback log exception. will reopen the ReplicatedEnvironment.", rollbackEx);
                bdbEnvironment.close();
                bdbEnvironment.openReplicatedEnvironment(new File(environmentPath));
            }
        }
    }

    private void reSetupBdbEnvironment(InsufficientLogException insufficientLogEx) {
        LOG.warn("catch insufficient log exception. will recover and try again.", insufficientLogEx);
        // Copy the missing log files from a member of the replication group who owns
        // the files
        // ATTN: here we use `getServingEnv()`, because only serving catalog has
        // helper nodes.
        HostInfo helperNode = Env.getServingEnv().getHelperNode();

        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                NetworkRestore restore = new NetworkRestore();
                NetworkRestoreConfig config = new NetworkRestoreConfig();
                config.setRetainLogFiles(false);
                restore.execute(insufficientLogEx, config);
                break;
            } catch (Exception e) {
                LOG.warn("retry={}, reSetupBdbEnvironment exception:", i, e);
                try {
                    Thread.sleep(5 * 1000);
                    LOG.warn("after sleep insufficientLogEx:", insufficientLogEx);
                } catch (InterruptedException e1) {
                    LOG.warn("InterruptedException", e1);
                }
            }
        }

        bdbEnvironment.close();
        bdbEnvironment.setup(new File(environmentPath), selfNodeName, selfNodeHostPort,
                NetUtils.getHostPortInAccessibleFormat(helperNode.getHost(), helperNode.getPort()));
    }

    @Override
    public long getJournalNum() {
        return currentJournalDB.count();
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

        // Open a new journal database or get last existing one as current journal
        // database
        List<Long> dbNames = null;
        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                dbNames = bdbEnvironment.getDatabaseNames();
                break;
            } catch (InsufficientLogException insufficientLogEx) {
                /*
                 * If this is not a checkpoint thread, which means this maybe the FE startup
                 * thread,
                 * or a replay thread. We will reopen bdbEnvironment for these 2 cases to get
                 * valid log
                 * from helper nodes.
                 *
                 * The checkpoint thread will only run on Master FE. And Master FE should not
                 * encounter
                 * these exception. So if it happens, throw exception out.
                 */
                if (!Env.isCheckpointThread()) {
                    reSetupBdbEnvironment(insufficientLogEx);
                } else {
                    throw insufficientLogEx;
                }
            } catch (RollbackException rollbackEx) {
                if (!Env.isCheckpointThread()) {
                    LOG.warn("catch rollback log exception. will reopen the ReplicatedEnvironment.", rollbackEx);
                    bdbEnvironment.close();
                    bdbEnvironment.openReplicatedEnvironment(new File(environmentPath));
                } else {
                    throw rollbackEx;
                }
            }
        }

        return dbNames;
    }

    public BDBEnvironment getBDBEnvironment() {
        return this.bdbEnvironment;
    }

    public String getBDBStats() {
        if (bdbEnvironment == null) {
            return "";
        }

        ReplicatedEnvironment repEnv = bdbEnvironment.getReplicatedEnvironment();
        if (repEnv == null) {
            return "";
        }

        return repEnv.getRepStats(StatsConfig.DEFAULT).toString();
    }

    public String getNotReadyReason() {
        if (bdbEnvironment == null) {
            LOG.warn("replicatedEnvironment is null");
            return "replicatedEnvironment is null";
        }
        return bdbEnvironment.getNotReadyReason();
    }
}
