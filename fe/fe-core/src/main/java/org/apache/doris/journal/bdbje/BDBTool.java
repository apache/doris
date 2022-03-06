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
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.meta.MetaContext;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

public class BDBTool {

    private String metaPath;
    private BDBToolOptions options;

    public BDBTool(String metaPath, BDBToolOptions options) {
        this.metaPath = metaPath;
        this.options = options;
    }

    public boolean run() {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(false);
        envConfig.setReadOnly(true);
        envConfig.setCachePercent(20);

        Environment env = null;
        try {
            env = new Environment(new File(metaPath), envConfig);
        } catch (DatabaseException e) {
            e.printStackTrace();
            System.err.println("Failed to open BDBJE env: " + Catalog.getCurrentCatalog().getBdbDir() + ". exit");
            return false;
        }
        Preconditions.checkNotNull(env);

        try {
            if (options.isListDbs()) {
                // list all databases
                List<String> dbNames = env.getDatabaseNames();
                System.out.println(JSONArray.toJSONString(dbNames));
                return true;
            } else {
                // db operations
                String dbName = options.getDbName();
                Preconditions.checkState(!Strings.isNullOrEmpty(dbName));
                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setAllowCreate(false);
                dbConfig.setReadOnly(true);
                Database db = env.openDatabase(null, dbName, dbConfig);

                if (options.isDbStat()) {
                    // get db stat
                    Map<String, String> statMap = Maps.newHashMap();
                    statMap.put("count", String.valueOf(db.count()));
                    System.out.println(JSONObject.toJSONString(statMap));
                    return true;
                } else {
                    // set from key
                    Long fromKey = 0L;
                    String fromKeyStr = options.hasFromKey() ? options.getFromKey() : dbName;
                    try {
                        fromKey = Long.valueOf(fromKeyStr);
                    } catch (NumberFormatException e) {
                        System.err.println("Not a valid from key: " + fromKeyStr);
                        return false;
                    }
                    
                    // set end key
                    Long endKey = fromKey + db.count() - 1;
                    if (options.hasEndKey()) {
                        try {
                            endKey = Long.valueOf(options.getEndKey());
                        } catch (NumberFormatException e) {
                            System.err.println("Not a valid end key: " + options.getEndKey());
                            return false;
                        }
                    }
                    
                    if (fromKey > endKey) {
                        System.err.println("from key should less than or equal to end key["
                                + fromKey + " vs. " + endKey + "]");
                        return false;
                    }
                    
                    // meta version
                    MetaContext metaContext = new MetaContext();
                    metaContext.setMetaVersion(options.getMetaVersion());
                    metaContext.setThreadLocalInfo();

                    for (Long key = fromKey; key <= endKey; key++) {
                        getValueByKey(db, key);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to run bdb tools");
            return false;
        }
        return true;
    }

    private void getValueByKey(Database db, Long key)
            throws UnsupportedEncodingException {

        DatabaseEntry queryKey = new DatabaseEntry();
        TupleBinding<Long> myBinding = TupleBinding.getPrimitiveBinding(Long.class);
        myBinding.objectToEntry(key, queryKey);
        DatabaseEntry value = new DatabaseEntry();

        OperationStatus status = db.get(null, queryKey, value, LockMode.READ_COMMITTED);
        if (status == OperationStatus.SUCCESS) {
            byte[] retData = value.getData();
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(retData));
            JournalEntity entity = new JournalEntity();
            try {
                entity.readFields(in);
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Fail to read journal entity for key: " + key + ". reason: " + e.getMessage());
                System.exit(-1);
            }
            System.out.println("key: " + key);
            System.out.println("op code: " + entity.getOpCode());
            System.out.println("value: " + entity.getData().toString());
        } else if (status == OperationStatus.NOTFOUND) {
            System.out.println("key: " + key);
            System.out.println("value: NOT FOUND");
        }
    }
}
