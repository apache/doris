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

package org.apache.doris.datasource.iceberg.dlf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.io.FileIO;
import shade.doris.hive.org.apache.thrift.TException;

public class DLFTableOperations extends HiveTableOperations {

    private final ClientPool<IMetaStoreClient, TException> metaClients;
    private final String database;
    private final String tableName;
    private final int metadataRefreshMaxRetries;

    public DLFTableOperations(Configuration conf,
                              ClientPool<IMetaStoreClient, TException> metaClients,
                              FileIO fileIO,
                              String catalogName,
                              String database,
                              String table) {
        super(conf, metaClients, fileIO, catalogName, database, table);
        this.metaClients = metaClients;
        this.database = database;
        this.tableName = table;
        metadataRefreshMaxRetries = conf.getInt(
            "iceberg.hive.metadata-refresh-max-retries", 2);
    }

    @Override
    protected void doRefresh() {
        String metadataLocation = null;
        try {
            Table table = metaClients.run(client -> client.getTable(database, tableName));
            metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
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
}
