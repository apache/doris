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

package org.apache.doris.datasource;

import org.apache.doris.catalog.AuthType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HMSResource;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.HMSExternalDatabase;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.hive.event.MetastoreNotificationFetchException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * External catalog for hive metastore compatible data sources.
 */
public class HMSExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(HMSExternalCatalog.class);

    private static final int MAX_CLIENT_POOL_SIZE = 8;
    protected PooledHiveMetaStoreClient client;
    // Record the latest synced event id when processing hive events
    private long lastSyncedEventId = -1;

    /**
     * Default constructor for HMSExternalCatalog.
     */
    public HMSExternalCatalog(
            long catalogId, String name, String resource, Map<String, String> props) throws DdlException {
        this.id = catalogId;
        this.name = name;
        this.type = "hms";
        if (resource == null) {
            props.putAll(HMSResource.getPropertiesFromDLF());
        }
        catalogProperty = new CatalogProperty(resource, props);
        // TODO: 2022/12/27 判断是否开启同步
        //        if (enableHmsEventsIncrementalSync) {
        setLastSyncedEventId(getCurrentEventId());
        //        }
    }

    public String getHiveMetastoreUris() {
        return catalogProperty.getOrDefault(HMSResource.HIVE_METASTORE_URIS, "");
    }

    @Override
    public void notifyPropertiesUpdated() {
        initLocalObjectsImpl();
    }

    @Override
    public List<String> listDatabaseNames(SessionContext ctx) {
        makeSureInitialized();
        return Lists.newArrayList(dbNameToId.keySet());
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        HMSExternalDatabase hmsExternalDatabase = (HMSExternalDatabase) idToDb.get(dbNameToId.get(dbName));
        if (hmsExternalDatabase != null && hmsExternalDatabase.isInitialized()) {
            List<String> names = Lists.newArrayList();
            hmsExternalDatabase.getTables().forEach(table -> names.add(table.getName()));
            return names;
        } else {
            return client.getAllTables(getRealTableName(dbName));
        }
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        return client.tableExists(getRealTableName(dbName), tblName);
    }

    @Override
    protected void initLocalObjectsImpl() {
        HiveConf hiveConf = new HiveConf();
        for (Map.Entry<String, String> kv : catalogProperty.getHadoopProperties().entrySet()) {
            hiveConf.set(kv.getKey(), kv.getValue());
        }

        String authentication = catalogProperty.getOrDefault(
                HdfsResource.HADOOP_SECURITY_AUTHENTICATION, "");
        if (AuthType.KERBEROS.getDesc().equals(authentication)) {
            Configuration conf = new Configuration();
            conf.set(HdfsResource.HADOOP_SECURITY_AUTHENTICATION, authentication);
            UserGroupInformation.setConfiguration(conf);
            try {
                /**
                 * Because metastore client is created by using
                 * {@link org.apache.hadoop.hive.metastore.RetryingMetaStoreClient#getProxy}
                 * it will relogin when TGT is expired, so we don't need to relogin manually.
                 */
                UserGroupInformation.loginUserFromKeytab(
                        catalogProperty.getOrDefault(HdfsResource.HADOOP_KERBEROS_PRINCIPAL, ""),
                        catalogProperty.getOrDefault(HdfsResource.HADOOP_KERBEROS_KEYTAB, ""));
            } catch (IOException e) {
                throw new HMSClientException("login with kerberos auth failed for catalog %s", e, this.getName());
            }
        }

        client = new PooledHiveMetaStoreClient(hiveConf, MAX_CLIENT_POOL_SIZE);
    }

    @Override
    protected void init() {
        Map<String, Long> tmpDbNameToId = Maps.newConcurrentMap();
        Map<Long, ExternalDatabase> tmpIdToDb = Maps.newConcurrentMap();
        InitCatalogLog initCatalogLog = new InitCatalogLog();
        initCatalogLog.setCatalogId(id);
        initCatalogLog.setType(InitCatalogLog.Type.HMS);
        List<String> allDatabases = client.getAllDatabases();
        // Update the db name to id map.
        for (String dbName : allDatabases) {
            long dbId;
            if (dbNameToId != null && dbNameToId.containsKey(dbName)) {
                dbId = dbNameToId.get(dbName);
                tmpDbNameToId.put(dbName, dbId);
                ExternalDatabase db = idToDb.get(dbId);
                db.setUnInitialized(invalidCacheInInit);
                tmpIdToDb.put(dbId, db);
                initCatalogLog.addRefreshDb(dbId);
            } else {
                dbId = Env.getCurrentEnv().getNextId();
                tmpDbNameToId.put(dbName, dbId);
                HMSExternalDatabase db = new HMSExternalDatabase(this, dbId, dbName);
                tmpIdToDb.put(dbId, db);
                initCatalogLog.addCreateDb(dbId, dbName);
            }
        }
        dbNameToId = tmpDbNameToId;
        idToDb = tmpIdToDb;
        Env.getCurrentEnv().getEditLog().logInitCatalog(initCatalogLog);
    }

    @Override
    public List<Column> getSchema(String dbName, String tblName) {
        makeSureInitialized();
        List<FieldSchema> schema = getClient().getSchema(dbName, tblName);
        List<Column> tmpSchema = Lists.newArrayListWithCapacity(schema.size());
        for (FieldSchema field : schema) {
            tmpSchema.add(new Column(field.getName(),
                    HiveMetaStoreClientHelper.hiveTypeToDorisType(field.getType()), true, null,
                    true, null, field.getComment(), true, null, -1));
        }
        return tmpSchema;
    }

    public PooledHiveMetaStoreClient getClient() {
        makeSureInitialized();
        return client;
    }

    public void setLastSyncedEventId(long lastSyncedEventId) {
        this.lastSyncedEventId = lastSyncedEventId;
    }

    public NotificationEventResponse getNextEventResponse(String catalogName)
            throws MetastoreNotificationFetchException {
        if (lastSyncedEventId == -1) {
            lastSyncedEventId = getCurrentEventId();
            LOG.error("Last synced event id is null when pulling events on catalog [{}]", catalogName);
            return null;
        }

        long currentEventId = getCurrentEventId();
        if (currentEventId == lastSyncedEventId) {
            LOG.info("Event id not updated when pulling events on catalog [{}]", catalogName);
            return null;
        }
        return client.getNextNotification(lastSyncedEventId, Config.hms_events_batch_size_per_rpc, null);
    }

    public long getCurrentEventId() {
        return client.getCurrentNotificationEventId().getEventId();
    }
}
