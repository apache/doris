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

import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.common.Config;

import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * A hive metastore client pool for a specific catalog with hive configuration.
 */
public class PooledHiveMetaStoreClient {
    private static final Logger LOG = LogManager.getLogger(PooledHiveMetaStoreClient.class);

    private static final HiveMetaHookLoader DUMMY_HOOK_LOADER = t -> null;
    private static final short MAX_LIST_PARTITION_NUM = 10000;

    private Queue<CachedClient> clientPool = new LinkedList<>();
    private final int poolSize;
    private final HiveConf hiveConf;

    public PooledHiveMetaStoreClient(HiveConf hiveConf, int pooSize) {
        Preconditions.checkArgument(pooSize > 0, pooSize);
        hiveConf.set(ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.name(),
                String.valueOf(Config.hive_metastore_client_timeout_second));
        this.hiveConf = hiveConf;
        this.poolSize = pooSize;
    }

    public List<String> getAllDatabases() {
        try (CachedClient client = getClient()) {
            return client.client.getAllDatabases();
        } catch (Exception e) {
            throw new HMSClientException("failed to get all database from hms client", e);
        }
    }

    public List<String> getAllTables(String dbName) {
        try (CachedClient client = getClient()) {
            return client.client.getAllTables(dbName);
        } catch (Exception e) {
            throw new HMSClientException("failed to get all tables for db %s", e, dbName);
        }
    }

    public boolean tableExists(String dbName, String tblName) {
        try (CachedClient client = getClient()) {
            return client.client.tableExists(dbName, tblName);
        } catch (Exception e) {
            throw new HMSClientException("failed to check if table %s in db %s exists", e, tblName, dbName);
        }
    }

    public List<String> listPartitionNames(String dbName, String tblName) {
        try (CachedClient client = getClient()) {
            return client.client.listPartitionNames(dbName, tblName, MAX_LIST_PARTITION_NUM);
        } catch (Exception e) {
            throw new HMSClientException("failed to list partition names for table %s in db %s", e, tblName, dbName);
        }
    }

    public Partition getPartition(String dbName, String tblName, List<String> partitionValues) {
        try (CachedClient client = getClient()) {
            return client.client.getPartition(dbName, tblName, partitionValues);
        } catch (Exception e) {
            throw new HMSClientException("failed to get partition for table %s in db %s with value %s", e, tblName,
                    dbName, partitionValues);
        }
    }

    public List<Partition> getPartitionsByFilter(String dbName, String tblName, String filter) {
        try (CachedClient client = getClient()) {
            return client.client.listPartitionsByFilter(dbName, tblName, filter, MAX_LIST_PARTITION_NUM);
        } catch (Exception e) {
            throw new HMSClientException("failed to get partition by filter for table %s in db %s", e, tblName,
                    dbName);
        }
    }

    public Table getTable(String dbName, String tblName) {
        try (CachedClient client = getClient()) {
            return client.client.getTable(dbName, tblName);
        } catch (Exception e) {
            throw new HMSClientException("failed to get table %s in db %s from hms client", e, tblName, dbName);
        }
    }

    public List<FieldSchema> getSchema(String dbName, String tblName) {
        try (CachedClient client = getClient()) {
            return client.client.getSchema(dbName, tblName);
        } catch (Exception e) {
            throw new HMSClientException("failed to get schema for table %s in db %s", e, tblName, dbName);
        }
    }

    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tblName, List<String> columns) {
        try (CachedClient client = getClient()) {
            return client.client.getTableColumnStatistics(dbName, tblName, columns);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName, String tblName, List<String> partNames, List<String> columns) {
        try (CachedClient client = getClient()) {
            return client.client.getPartitionColumnStatistics(dbName, tblName, partNames, columns);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private class CachedClient implements AutoCloseable {
        private final IMetaStoreClient client;

        private CachedClient(HiveConf hiveConf) throws MetaException {
            String type = hiveConf.get(HiveMetaStoreClientHelper.HIVE_METASTORE_TYPE);
            if (HiveMetaStoreClientHelper.DLF_TYPE.equalsIgnoreCase(type)) {
                client = RetryingMetaStoreClient.getProxy(hiveConf, DUMMY_HOOK_LOADER,
                        ProxyMetaStoreClient.class.getName());
            } else {
                client = RetryingMetaStoreClient.getProxy(hiveConf, DUMMY_HOOK_LOADER,
                        HiveMetaStoreClient.class.getName());
            }
        }

        @Override
        public void close() throws Exception {
            synchronized (clientPool) {
                if (clientPool.size() > poolSize) {
                    client.close();
                } else {
                    clientPool.offer(this);
                }
            }
        }
    }

    private CachedClient getClient() throws MetaException {
        synchronized (clientPool) {
            CachedClient client = clientPool.poll();
            if (client == null) {
                return new CachedClient(hiveConf);
            }
            return client;
        }
    }
}
