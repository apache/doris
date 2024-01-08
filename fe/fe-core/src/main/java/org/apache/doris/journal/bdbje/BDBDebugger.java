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

import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.httpv2.HttpServer;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.qe.QeService;
import org.apache.doris.service.ExecuteEnv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.util.List;

/**
 * This class is mainly used to view the data in bdbje.
 * When the config "enable_bdbje_debug_mode" of FE is set to true,
 * start FE and enter debug mode.
 * In this mode, only MySQL server and http server will be started.
 * After that, users can log in to Palo through the web front-end or MySQL client,
 * and then use "show proc "/bdbje"" to view the data in bdbje.
 */
public class BDBDebugger {
    private static final Logger LOG = LogManager.getLogger(BDBDebugger.class);
    private BDBDebugEnv debugEnv;

    private static class SingletonHolder {
        private static final BDBDebugger INSTANCE = new BDBDebugger();
    }

    public static BDBDebugger get() {
        return SingletonHolder.INSTANCE;
    }

    /**
     * Start in BDB Debug mode.
     */
    public void startDebugMode(String bdbHome) {
        try {
            initDebugEnv(bdbHome);
            startService();
            while (true) {
                Thread.sleep(2000);
            }
        } catch (Throwable t) {
            LOG.warn("BDB debug mode exception", t);
            System.exit(-1);
        }
    }

    private void initDebugEnv(String bdbHome) throws BDBDebugException {
        debugEnv = new BDBDebugEnv(bdbHome);
        debugEnv.init();
    }

    // Only start MySQL and HttpServer
    private void startService() throws Exception {
        // HTTP server

        HttpServer httpServer = new HttpServer();
        httpServer.setPort(Config.http_port);
        httpServer.start();

        // MySQl server
        QeService qeService = new QeService(Config.query_port, Config.arrow_flight_sql_port,
                                            ExecuteEnv.getInstance().getScheduler());
        qeService.start();

        ThreadPoolManager.registerAllThreadPoolMetric();
    }

    public BDBDebugEnv getEnv() {
        return debugEnv;
    }

    /**
     * A wrapper class of the BDBJE environment, used to obtain information in bdbje.
     */
    public static class BDBDebugEnv {
        // the dir of bdbje data dir
        private String metaPath;
        private Environment env;

        public BDBDebugEnv(String metaPath) {
            this.metaPath = metaPath;
        }

        public void init() throws BDBDebugException {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(false);
            envConfig.setReadOnly(true);
            envConfig.setCachePercent(20);

            try {
                env = new Environment(new File(metaPath), envConfig);
            } catch (DatabaseException e) {
                throw new BDBDebugException("failed to init bdb env", e);
            }
            Preconditions.checkNotNull(env);
        }

        // list all database in bdbje
        public List<String> listDbNames() {
            return env.getDatabaseNames();
        }

        // get the number of journal in specified database
        public Long getJournalNumber(String dbName) {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(false);
            dbConfig.setReadOnly(true);
            Database db = env.openDatabase(null, dbName, dbConfig);
            long journalNumber = db.count();
            db.close();
            return journalNumber;
        }

        /**
         * get list of journal id (key) in specified database.
         */
        public List<Long> getJournalIds(String dbName) throws BDBDebugException {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(false);
            dbConfig.setReadOnly(true);
            Database db = env.openDatabase(null, dbName, dbConfig);

            List<Long> journalIds = Lists.newArrayList();

            Cursor cursor = db.openCursor(null, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            TupleBinding<Long> idBinding = TupleBinding.getPrimitiveBinding(Long.class);
            try {
                while (true) {
                    OperationStatus status = cursor.getNext(key, value, null);
                    if (status == OperationStatus.NOTFOUND) {
                        break;
                    }

                    Long id = idBinding.entryToObject(key);
                    journalIds.add(id);
                }
                cursor.close();
                db.close();
            } catch (Exception e) {
                LOG.warn("failed to get journal ids of {}", dbName, e);
                throw new BDBDebugException("failed to get journal ids of database " + dbName, e);
            }
            return journalIds;
        }

        /**
         * get the journal entity of the specified journal id.
         */
        public JournalEntityWrapper getJournalEntity(String dbName, Long journalId) {
            // meta version
            // TODO(cmy): currently the journal data will be read with VERSION_CURRENT
            // So if we use a new version of Palo to read the meta of old version,
            // It may fail.
            MetaContext metaContext = new MetaContext();
            metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
            metaContext.setThreadLocalInfo();

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(false);
            dbConfig.setReadOnly(true);
            Database db = env.openDatabase(null, dbName, dbConfig);

            JournalEntityWrapper entityWrapper = new JournalEntityWrapper(journalId);

            DatabaseEntry key = new DatabaseEntry();
            TupleBinding<Long> myBinding = TupleBinding.getPrimitiveBinding(Long.class);
            myBinding.objectToEntry(journalId, key);
            DatabaseEntry value = new DatabaseEntry();

            // get the journal
            OperationStatus status = db.get(null, key, value, LockMode.READ_COMMITTED);
            db.close();
            if (status == OperationStatus.SUCCESS) {
                byte[] retData = value.getData();
                entityWrapper.size = retData.length;
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(retData));
                JournalEntity entity = new JournalEntity();
                try {
                    // read the journal data
                    entity.readFields(in);
                    entityWrapper.entity = entity;
                } catch (Exception e) {
                    LOG.warn("failed to read entity", e);
                    entityWrapper.errMsg = e.getMessage();
                }
            } else if (status == OperationStatus.NOTFOUND) {
                entityWrapper.errMsg = "Key not found";
            }
            MetaContext.remove();
            return entityWrapper;
        }

        public void close() {
            try {
                env.close();
            } catch (Exception e) {
                LOG.warn("exception:", e);
            }
        }
    }

    public static class JournalEntityWrapper {
        public Long journalId;
        public Integer size;
        public JournalEntity entity;
        public String errMsg;

        public JournalEntityWrapper(long journalId) {
            this.journalId = journalId;
        }
    }

    /**
     * BDBDebugException.
     */
    public static class BDBDebugException extends Exception {
        public BDBDebugException(String msg) {
            super(msg);
        }

        public BDBDebugException(String msg, Throwable t) {
            super(msg, t);
        }
    }
}
