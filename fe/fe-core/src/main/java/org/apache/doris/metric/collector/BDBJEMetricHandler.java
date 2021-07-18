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

package org.apache.doris.metric.collector;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.journal.bdbje.BDBEnvironment;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Put;
import com.sleepycat.je.WriteOptions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;

/**
 * This class stores the metric data of fe and be into the database of BDBEnvironment.
 */
public class BDBJEMetricHandler {
    private static final Logger LOG = LogManager.getLogger(BDBJEMetricHandler.class);
    private static final String CHARSET_NAME = "UTF-8";
    private static final String METRIC_DB_NAME = "metricDB";

    private BDBEnvironment bdbEnvironment;
    // the metricDb maybe null at runtime, visit it by using getMetricDb()
    private Database metricDb;
    private WriteOptions wo;

    private EntryBinding longTupleBinding = TupleBinding.getPrimitiveBinding(Long.class);
    private EntryBinding doubleTupleBinding = TupleBinding.getPrimitiveBinding(Double.class);

    public BDBJEMetricHandler(BDBEnvironment bdbEnv) {
        bdbEnvironment = bdbEnv;
        initMetricDb();
        wo = new WriteOptions();
        wo.setTTL(Config.metric_ttl);
    }

    private void initMetricDb() {
        if (metricDb != null) {
            return;
        }
        synchronized (this) {
            if (metricDb != null) {
                return;
            }
            try {
                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setTransactional(true);
                if (Catalog.getCurrentCatalog().isElectable()) {
                    dbConfig.setAllowCreate(true).setReadOnly(false).setKeyPrefixing(true);
                } else {
                    dbConfig.setAllowCreate(false).setReadOnly(true);
                }
                // Configure the METRIC_DB_NAME to support key prefixing.
                metricDb = bdbEnvironment.openDatabase(METRIC_DB_NAME, dbConfig);
                // metricDb maybe null after we open database.
                // When we upgrade the cluster from version 0.13, we usually upgrade
                // the non-Master FE, such as Observer first.
                // At this time, MetricDb does not exist in BDBJE, so openDatabase()
                // will report an error that "DB cannot be found", but this error will not be thrown,
                // but just make openDatabase() return null.
            } catch (Exception e) {
                LOG.warn("open metric bdbje database error.", e);
            }
        }
    }

    private Database getMetricDb() throws UserException {
        initMetricDb();
        if (metricDb == null) {
            throw new UserException("MetricDb is not opened");
        }
        return metricDb;
    }

    public void writeLong(String key, long value) {
        DatabaseEntry theValue = new DatabaseEntry();
        longTupleBinding.objectToEntry(value, theValue);
        write(key, theValue);
    }

    public void writeDouble(String key, double value) {
        DatabaseEntry theValue = new DatabaseEntry();
        doubleTupleBinding.objectToEntry(value, theValue);
        write(key, theValue);
    }

    private void write(String key, DatabaseEntry theValue) {
        try {
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes(CHARSET_NAME));
            getMetricDb().put(null, theKey, theValue, Put.OVERWRITE, wo);
        } catch (Exception e) {
            LOG.warn("write metric data into bdb error, key:{}", key, e);
        }
    }

    // if you use writeLong() method to write entry, you must use readLong() to read it.
    public Long readLong(String key) {
        try {
            Long result = (Long) longTupleBinding.entryToObject(read(key));
            return result;
        } catch (IndexOutOfBoundsException e) {
            // An IndexOutOfBoundsException will be thrown when the queried data does not exist.
            return null;
        } catch (UnsupportedEncodingException e) {
            // rarely happen, just make compiler happy
            return null;
        } catch (UserException e) {
            LOG.warn("failed to read key: {}", key, e);
            return null;
        }
    }

    // if you use writeInt() method to write entry, you must use readInt() to read it.
    public Double readDouble(String key) {
        try {
            Double result = (Double) doubleTupleBinding.entryToObject(read(key));
            return result;
        } catch (Exception e) {
            // An IndexOutOfBoundsException will be thrown when the queried data does not exist.
            return null;
        }
    }

    private DatabaseEntry read(String key) throws UnsupportedEncodingException, UserException {
        DatabaseEntry theKey = new DatabaseEntry(key.getBytes(CHARSET_NAME));
        DatabaseEntry theValue = new DatabaseEntry();
        getMetricDb().get(null, theKey, theValue, LockMode.DEFAULT);
        return theValue;
    }

    public void close() {
        if (metricDb != null) {
            metricDb.close();
        }
    }
}