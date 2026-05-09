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

package org.apache.doris.datasource.iceberg.hive;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import shade.doris.hive.org.apache.thrift.TException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class HiveTableOperations extends BaseMetastoreTableOperations {
    private static final String HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES = "iceberg.hive.metadata-refresh-max-retries";
    private static final String HIVE_TABLE_LEVEL_LOCK_EVICT_MS = "iceberg.hive.table-level-lock-evict-ms";
    private static final int HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES_DEFAULT = 2;
    private static final long HIVE_TABLE_LEVEL_LOCK_EVICT_MS_DEFAULT = TimeUnit.MINUTES.toMillis(10);

    private static Cache<String, ReentrantLock> commitLockCache;

    private static synchronized void initTableLevelLockCache(long evictionTimeout) {
        if (commitLockCache == null) {
            commitLockCache = Caffeine.newBuilder()
                .expireAfterAccess(evictionTimeout, TimeUnit.MILLISECONDS)
                .build();
        }
    }

    private final String fullName;
    private final String database;
    private final String tableName;
    private final Configuration conf;
    private final int metadataRefreshMaxRetries;
    private final FileIO fileIO;
    private final ClientPool<IMetaStoreClient, TException> metaClients;

    public HiveTableOperations(Configuration conf, ClientPool metaClients, FileIO fileIO,
                               String catalogName, String database, String table) {
        this.conf = conf;
        this.metaClients = metaClients;
        this.fileIO = fileIO;
        this.fullName = catalogName + "." + database + "." + table;
        this.database = database;
        this.tableName = table;
        this.metadataRefreshMaxRetries =
                conf.getInt(HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES,
                HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES_DEFAULT);
        long tableLevelLockCacheEvictionTimeout =
                conf.getLong(HIVE_TABLE_LEVEL_LOCK_EVICT_MS, HIVE_TABLE_LEVEL_LOCK_EVICT_MS_DEFAULT);
        initTableLevelLockCache(tableLevelLockCacheEvictionTimeout);
    }

    @Override
    protected String tableName() {
        return fullName;
    }

    @Override
    public FileIO io() {
        return fileIO;
    }

    @Override
    protected void doRefresh() {
        String metadataLocation = null;
        try {
            Table table = metaClients.run(client -> client.getTable(database, tableName));
            validateTableIsIceberg(table, fullName);

            metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
            if (Strings.isNullOrEmpty(metadataLocation)) {
                throw new NotFoundException("Property 'metadata_location' can not be found in table %s.%s. "
                    + "Probably this table is created by Spark2, which is not supported.", database, tableName);
            }

        } catch (NoSuchObjectException e) {
            if (currentMetadataLocation() != null) {
                throw new NoSuchTableException("No such table: %s.%s", database, tableName);
            }

        } catch (TException e) {
            String errMsg = String.format("Failed to get table info from metastore %s.%s", database, tableName);
            throw new RuntimeException(errMsg, e);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during refresh", e);
        }

        refreshFromMetadataLocation(metadataLocation, metadataRefreshMaxRetries);
    }

    @Override
    protected void doCommit(TableMetadata base, TableMetadata metadata) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public static void validateTableIsIceberg(Table table, String fullName) {
        String tableType = table.getParameters().get(TABLE_TYPE_PROP);
        Preconditions.checkState(tableType != null && tableType.equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE),
                "Not an iceberg table: %s (type=%s)", fullName, tableType);
    }
}
