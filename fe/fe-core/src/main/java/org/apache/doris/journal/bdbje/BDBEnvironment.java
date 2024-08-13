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
import org.apache.doris.common.Config;
import org.apache.doris.ha.BDBHA;
import org.apache.doris.ha.BDBStateChangeListener;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.ha.HAProtocol;
import org.apache.doris.system.Frontend;

import com.google.common.collect.ImmutableList;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.util.DbResetRepGroup;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.stream.Collectors;

/* this class contains the reference to bdb environment.
 * including all the opened databases and the replicationGroupAdmin.
 * we can get the information of this bdb group through the API of replicationGroupAdmin
 */
public class BDBEnvironment {
    private static final Logger LOG = LogManager.getLogger(BDBEnvironment.class);
    private static final int RETRY_TIME = 3;
    private static final int MEMORY_CACHE_PERCENT = 20;
    private static final List<String> BDBJE_LOG_LEVEL = ImmutableList.of("OFF", "SEVERE", "WARNING",
            "INFO", "CONFIG", "FINE", "FINER", "FINEST", "ALL");
    public static final String PALO_JOURNAL_GROUP = "PALO_JOURNAL_GROUP";

    private ReplicatedEnvironment replicatedEnvironment;
    private EnvironmentConfig environmentConfig;
    private ReplicationConfig replicationConfig;
    private DatabaseConfig dbConfig;
    private Database epochDB = null;  // used for fencing
    private ReentrantReadWriteLock lock;
    private List<Database> openedDatabases;

    private final boolean isElectable;
    private final boolean metadataFailureRecovery;

    public BDBEnvironment(boolean isElectable, boolean metadataFailureRecovery) {
        openedDatabases = new ArrayList<Database>();
        this.lock = new ReentrantReadWriteLock(true);
        this.isElectable = isElectable;
        this.metadataFailureRecovery = metadataFailureRecovery;
    }

    // The setup() method opens the environment and database
    public void setup(File envHome, String selfNodeName, String selfNodeHostPort,
                      String helperHostPort) {
        // Almost never used, just in case the master can not restart
        if (metadataFailureRecovery || Config.enable_check_compatibility_mode) {
            if (!isElectable && !Config.enable_check_compatibility_mode) {
                LOG.error("Current node is not in the electable_nodes list. will exit");
                System.exit(-1);
            }
            LOG.warn("start group reset");
            DbResetRepGroup resetUtility = new DbResetRepGroup(
                    envHome, PALO_JOURNAL_GROUP, selfNodeName, selfNodeHostPort);
            resetUtility.reset();
            LOG.warn("metadata recovery mode, group has been reset.");
        }

        // set replication config
        replicationConfig = new ReplicationConfig();
        replicationConfig.setNodeName(selfNodeName);
        replicationConfig.setNodeHostPort(selfNodeHostPort);
        replicationConfig.setHelperHosts(helperHostPort);
        replicationConfig.setGroupName(PALO_JOURNAL_GROUP);
        replicationConfig.setConfigParam(ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT, "10 s");
        replicationConfig.setMaxClockDelta(Config.max_bdbje_clock_delta_ms, TimeUnit.MILLISECONDS);
        replicationConfig.setConfigParam(ReplicationConfig.TXN_ROLLBACK_LIMIT,
                String.valueOf(Config.txn_rollback_limit));
        replicationConfig.setConfigParam(ReplicationConfig.REPLICA_TIMEOUT,
                Config.bdbje_heartbeat_timeout_second + " s");
        replicationConfig.setConfigParam(ReplicationConfig.FEEDER_TIMEOUT,
                Config.bdbje_heartbeat_timeout_second + " s");

        if (isElectable) {
            replicationConfig.setReplicaAckTimeout(Config.bdbje_replica_ack_timeout_second, TimeUnit.SECONDS);
            replicationConfig.setConfigParam(ReplicationConfig.REPLICA_MAX_GROUP_COMMIT, "0");
            replicationConfig.setConsistencyPolicy(new NoConsistencyRequiredPolicy());
        } else {
            replicationConfig.setNodeType(NodeType.SECONDARY);
            replicationConfig.setConsistencyPolicy(new NoConsistencyRequiredPolicy());
        }

        // set environment config
        environmentConfig = new EnvironmentConfig();
        environmentConfig.setTransactional(true);
        environmentConfig.setAllowCreate(true);
        environmentConfig.setCachePercent(MEMORY_CACHE_PERCENT);
        environmentConfig.setLockTimeout(Config.bdbje_lock_timeout_second, TimeUnit.SECONDS);
        environmentConfig.setConfigParam(EnvironmentConfig.RESERVED_DISK,
                String.valueOf(Config.bdbje_reserved_disk_bytes));
        environmentConfig.setConfigParam(EnvironmentConfig.FREE_DISK,
                String.valueOf(Config.bdbje_free_disk_bytes));

        if (Config.ignore_bdbje_log_checksum_read) {
            environmentConfig.setConfigParam(EnvironmentConfig.LOG_CHECKSUM_READ, "false");
            LOG.warn("set EnvironmentConfig.LOG_CHECKSUM_READ false");
        }

        if (BDBJE_LOG_LEVEL.contains(Config.bdbje_file_logging_level)) {
            java.util.logging.Logger parent = java.util.logging.Logger.getLogger("com.sleepycat.je");
            parent.setLevel(Level.parse(Config.bdbje_file_logging_level));
            environmentConfig.setConfigParam(EnvironmentConfig.FILE_LOGGING_LEVEL, Config.bdbje_file_logging_level);
        } else {
            LOG.warn("bdbje_file_logging_level invalid value: {}, will not take effort, use default",
                    Config.bdbje_file_logging_level);
        }

        if (isElectable) {
            Durability durability = new Durability(getSyncPolicy(Config.master_sync_policy),
                    getSyncPolicy(Config.replica_sync_policy), getAckPolicy(Config.replica_ack_policy));
            environmentConfig.setDurability(durability);
        }

        // set database config
        dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        if (isElectable) {
            dbConfig.setAllowCreate(true);
            dbConfig.setReadOnly(false);
        } else {
            dbConfig.setAllowCreate(false);
            dbConfig.setReadOnly(true);
        }

        // open environment and epochDB
        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                if (replicatedEnvironment != null) {
                    this.close();
                }
                // open the environment
                replicatedEnvironment = new ReplicatedEnvironment(envHome, replicationConfig, environmentConfig);

                // get a BDBHA object and pass the reference to Catalog
                HAProtocol protocol = new BDBHA(this, selfNodeName);
                Env.getCurrentEnv().setHaProtocol(protocol);

                // start state change listener
                StateChangeListener listener = new BDBStateChangeListener(isElectable);
                replicatedEnvironment.setStateChangeListener(listener);
                // open epochDB. the first parameter null means auto-commit
                epochDB = replicatedEnvironment.openDatabase(null, "epochDB", dbConfig);
                break;
            } catch (InsufficientLogException insufficientLogEx) {
                LOG.info("i:{} insufficientLogEx:", i, insufficientLogEx);
                NetworkRestore restore = new NetworkRestore();
                NetworkRestoreConfig config = new NetworkRestoreConfig();
                config.setRetainLogFiles(false); // delete obsolete log files.
                // Use the members returned by insufficientLogEx.getLogProviders()
                // to select the desired subset of members and pass the resulting
                // list as the argument to config.setLogProviders(), if the
                // default selection of providers is not suitable.
                restore.execute(insufficientLogEx, config);
            } catch (DatabaseException e) {
                LOG.info("i:{} exception:", i, e);
                if (i < RETRY_TIME - 1) {
                    try {
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e1) {
                        LOG.warn("", e1);
                    }
                } else {
                    LOG.error("error to open replicated environment. will exit.", e);
                    System.exit(-1);
                }
            }
        }
    }

    public ReplicationGroupAdmin getReplicationGroupAdmin() {
        Set<InetSocketAddress> addresses = Env.getCurrentEnv()
                .getFrontends(FrontendNodeType.FOLLOWER)
                .stream()
                .filter(Frontend::isAlive)
                .map(fe -> new InetSocketAddress(fe.getHost(), fe.getEditLogPort()))
                .collect(Collectors.toSet());

        if (addresses.isEmpty()) {
            LOG.info("addresses is empty");
            return null;
        }
        return new ReplicationGroupAdmin(PALO_JOURNAL_GROUP, addresses);
    }

    // Return a handle to the epochDB
    public Database getEpochDB() {
        return epochDB;
    }

    // Return a handle to the environment
    public ReplicatedEnvironment getReplicatedEnvironment() {
        return replicatedEnvironment;
    }

    // return the database reference with the given name
    // also try to close previous opened database.
    public Database openDatabase(String dbName) {
        Database db = null;
        lock.writeLock().lock();
        try {
            // find if the specified database is already opened. find and return it.
            for (java.util.Iterator<Database> iter = openedDatabases.iterator(); iter.hasNext();) {
                Database openedDb = iter.next();
                try {
                    if (openedDb.getDatabaseName() == null) {
                        openedDb.close();
                        iter.remove();
                        continue;
                    }
                } catch (Exception e) {
                    /*
                     * In the case when 3 FE (1 master and 2 followers) start at same time,
                     * We may catch com.sleepycat.je.rep.DatabasePreemptedException which said that
                     * "Database xx has been forcibly closed in order to apply a replicated remove operation."
                     *
                     * Because when Master FE finished to save image, it try to remove old journals,
                     * and also remove the databases these old journals belongs to.
                     * So after Master removed the database from replicatedEnvironment,
                     * call db.getDatabaseName() will throw DatabasePreemptedException,
                     * because it has already been destroyed.
                     *
                     * The reason why Master can safely remove a database is because it knows that all
                     * non-master FE have already load the journal ahead of this database. So remove the
                     * database is safe.
                     *
                     * Here we just try to close the useless database(which may be removed by Master),
                     * so even we catch the exception, just ignore it is OK.
                     */
                    LOG.warn("get exception when try to close previously opened bdb database. ignore it", e);
                    iter.remove();
                    continue;
                }

                if (openedDb.getDatabaseName().equals(dbName)) {
                    return openedDb;
                }
            }

            // open the specified database.
            // the first parameter null means auto-commit
            try {
                db = replicatedEnvironment.openDatabase(null, dbName, dbConfig);
                openedDatabases.add(db);
            } catch (Exception e) {
                LOG.warn("catch an exception when open database {}", dbName, e);
            }
        } finally {
            lock.writeLock().unlock();
        }
        return db;
    }

    // close and remove the database whose name is dbName
    public void removeDatabase(String dbName) {
        lock.writeLock().lock();
        try {
            String targetDbName = null;
            int index = 0;
            for (Database db : openedDatabases) {
                String name = db.getDatabaseName();
                if (dbName.equals(name)) {
                    db.close();
                    LOG.info("database {} has been closed", name);
                    targetDbName = name;
                    break;
                }
                index++;
            }
            if (targetDbName != null) {
                LOG.info("begin to remove database {} from openedDatabases", targetDbName);
                openedDatabases.remove(index);
            }
            try {
                LOG.info("begin to remove database {} from replicatedEnvironment", dbName);
                // the first parameter null means auto-commit
                replicatedEnvironment.removeDatabase(null, dbName);
            } catch (DatabaseNotFoundException e) {
                LOG.warn("catch an exception when remove db:{}, this db does not exist", dbName, e);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // get journal db names and sort the names
    public List<Long> getDatabaseNames() {
        List<Long> ret = new ArrayList<Long>();
        List<String> names = null;
        int tried = 0;
        while (true) {
            try {
                names = replicatedEnvironment.getDatabaseNames();
                break;
            } catch (InsufficientLogException e) {
                throw e;
            } catch (RollbackException e) {
                throw e;
            } catch (EnvironmentFailureException e) {
                tried++;
                if (tried == RETRY_TIME) {
                    LOG.error("bdb environment failure exception.", e);
                    System.exit(-1);
                }
                LOG.warn("bdb environment failure exception. will retry", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    LOG.warn("", e1);
                }
            } catch (DatabaseException e) {
                LOG.warn("catch an exception when calling getDatabaseNames", e);
                return null;
            }
        }

        if (names != null) {
            for (String name : names) {
                if (StringUtils.isNumeric(name)) {
                    ret.add(Long.parseLong(name));
                } else {
                    if (LOG.isDebugEnabled()) {
                        // LOG.debug("get database names, skipped {}", name);
                    }
                }
            }
        }

        Collections.sort(ret);
        return ret;
    }

    // Close the store and environment
    public void close() {
        for (Database db : openedDatabases) {
            try {
                db.close();
            } catch (DatabaseException exception) {
                LOG.error("Error closing db {} will exit", db.getDatabaseName(), exception);
            }
        }
        openedDatabases.clear();

        if (epochDB != null) {
            try {
                epochDB.close();
                epochDB = null;
            } catch (DatabaseException exception) {
                LOG.error("Error closing db {} will exit", epochDB.getDatabaseName(), exception);
            }
        }

        if (replicatedEnvironment != null) {
            try {
                // Finally, close the store and environment.
                replicatedEnvironment.close();
                replicatedEnvironment = null;
            } catch (DatabaseException exception) {
                LOG.error("Error closing replicatedEnvironment", exception);
            }
        }
    }

    // open environment
    public void openReplicatedEnvironment(File envHome) {
        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                if (replicatedEnvironment != null) {
                    this.close();
                }
                // open the environment
                replicatedEnvironment =
                        new ReplicatedEnvironment(envHome, replicationConfig, environmentConfig);

                // start state change listener
                StateChangeListener listener = new BDBStateChangeListener(isElectable);
                replicatedEnvironment.setStateChangeListener(listener);

                // open epochDB. the first parameter null means auto-commit
                epochDB = replicatedEnvironment.openDatabase(null, "epochDB", dbConfig);
                break;
            } catch (DatabaseException e) {
                LOG.info("i:{} exception:", i, e);
                if (i < RETRY_TIME - 1) {
                    try {
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e1) {
                        LOG.warn("", e1);
                    }
                } else {
                    LOG.error("error to open replicated environment. will exit.", e);
                    System.exit(-1);
                }
            }
        }
    }

    private static SyncPolicy getSyncPolicy(String policy) {
        if (policy.equalsIgnoreCase("SYNC")) {
            return Durability.SyncPolicy.SYNC;
        }
        if (policy.equalsIgnoreCase("NO_SYNC")) {
            return Durability.SyncPolicy.NO_SYNC;
        }
        // default value is WRITE_NO_SYNC
        return Durability.SyncPolicy.WRITE_NO_SYNC;
    }

    private static ReplicaAckPolicy getAckPolicy(String policy) {
        if (policy.equalsIgnoreCase("ALL")) {
            return Durability.ReplicaAckPolicy.ALL;
        }
        if (policy.equalsIgnoreCase("NONE")) {
            return Durability.ReplicaAckPolicy.NONE;
        }
        // default value is SIMPLE_MAJORITY
        return Durability.ReplicaAckPolicy.SIMPLE_MAJORITY;
    }

    public String getNotReadyReason() {
        if (replicatedEnvironment == null) {
            LOG.warn("replicatedEnvironment is null");
            return "replicatedEnvironment is null";
        }
        try {
            if (replicatedEnvironment.getInvalidatingException() != null) {
                return replicatedEnvironment.getInvalidatingException().getMessage();
            }

            if (RepInternal.getNonNullRepImpl(replicatedEnvironment).getDiskLimitViolation() != null) {
                return RepInternal.getNonNullRepImpl(replicatedEnvironment).getDiskLimitViolation();
            }
        } catch (Exception e) {
            LOG.warn("getNotReadyReason exception:", e);
        }
        return "";
    }

}
