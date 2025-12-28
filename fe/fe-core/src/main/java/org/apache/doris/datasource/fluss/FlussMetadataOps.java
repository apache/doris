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

package org.apache.doris.datasource.fluss;

import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.operations.ExternalMetadataOps;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class FlussMetadataOps implements ExternalMetadataOps {
    private static final Logger LOG = LogManager.getLogger(FlussMetadataOps.class);

    protected Connection flussConnection;
    protected Admin flussAdmin;
    protected ExternalCatalog dorisCatalog;

    public FlussMetadataOps(ExternalCatalog dorisCatalog, Connection flussConnection) {
        this.dorisCatalog = dorisCatalog;
        this.flussConnection = flussConnection;
        this.flussAdmin = flussConnection.getAdmin();
    }

    @Override
    public void close() {
        // Connection lifecycle is managed by FlussExternalCatalog
    }

    @Override
    public boolean tableExist(String dbName, String tblName) {
        try {
            TablePath tablePath = TablePath.of(dbName, tblName);
            CompletableFuture<TableInfo> future = flussAdmin.getTableInfo(tablePath);
            future.get(); // Will throw exception if table doesn't exist
            return true;
        } catch (Exception e) {
            if (ExceptionUtils.getRootCause(e) instanceof TableNotExistException) {
                return false;
            }
            throw new RuntimeException("Failed to check table existence: " + dbName + "." + tblName, e);
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        try {
            CompletableFuture<List<String>> future = flussAdmin.listTables(dbName);
            List<String> tables = future.get();
            return tables != null ? tables : new ArrayList<>();
        } catch (Exception e) {
            LOG.warn("Failed to list tables for database: " + dbName, e);
            return new ArrayList<>();
        }
    }

    public TableInfo getTableInfo(String dbName, String tblName) {
        try {
            TablePath tablePath = TablePath.of(dbName, tblName);
            CompletableFuture<TableInfo> future = flussAdmin.getTableInfo(tablePath);
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get table info: " + dbName + "." + tblName, e);
        }
    }

    public Admin getAdmin() {
        return flussAdmin;
    }

    public Connection getConnection() {
        return flussConnection;
    }
}

