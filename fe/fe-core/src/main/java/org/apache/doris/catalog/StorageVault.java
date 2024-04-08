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

package org.apache.doris.catalog;

import org.apache.doris.analysis.CreateStorageVaultStmt;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.protobuf.TextFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class StorageVault {
    private static final Logger LOG = LogManager.getLogger(StorageVault.class);
    public static final String REFERENCE_SPLIT = "@";
    public static final String INCLUDE_DATABASE_LIST = "include_database_list";
    public static final String EXCLUDE_DATABASE_LIST = "exclude_database_list";
    public static final String LOWER_CASE_META_NAMES = "lower_case_meta_names";
    public static final String META_NAMES_MAPPING = "meta_names_mapping";

    public enum StorageVaultType {
        UNKNOWN,
        S3,
        HDFS;

        public static StorageVaultType fromString(String storageVaultTypeType) {
            for (StorageVaultType type : StorageVaultType.values()) {
                if (type.name().equalsIgnoreCase(storageVaultTypeType)) {
                    return type;
                }
            }
            return UNKNOWN;
        }
    }

    protected String name;
    protected StorageVaultType type;
    protected String id;
    private boolean ifNotExists;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public void writeLock() {
        lock.writeLock().lock();
    }

    public void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    public StorageVault() {
    }

    public StorageVault(String name, StorageVaultType type, boolean ifNotExists) {
        this.name = name;
        this.type = type;
        this.ifNotExists = ifNotExists;
    }

    public static StorageVault fromStmt(CreateStorageVaultStmt stmt) throws DdlException {
        return getStorageVaultInstance(stmt);
    }

    public boolean ifNotExists() {
        return this.ifNotExists;
    }


    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * Get StorageVault instance by StorageVault name and type
     * @param type
     * @param name
     * @return
     * @throws DdlException
     */
    private static StorageVault getStorageVaultInstance(CreateStorageVaultStmt stmt) throws DdlException {
        StorageVaultType type = stmt.getStorageVaultType();
        String name = stmt.getStorageVaultName();
        boolean ifNotExists = stmt.isIfNotExists();
        StorageVault vault;
        switch (type) {
            case HDFS:
                vault = new HdfsStorageVault(name, ifNotExists);
                break;
            default:
                throw new DdlException("Unknown StorageVault type: " + type);
        }
        vault.modifyProperties(stmt.getProperties());

        return vault;
    }

    public String getName() {
        return name;
    }

    public StorageVaultType getType() {
        return type;
    }

    /**
     * Modify properties in child resources
     * @param properties
     * @throws DdlException
     */
    public abstract void modifyProperties(Map<String, String> properties) throws DdlException;

    /**
     * Check properties in child resources
     * @param properties
     * @throws AnalysisException
     */
    public void checkProperties(Map<String, String> properties) throws AnalysisException { }

    protected void replaceIfEffectiveValue(Map<String, String> properties, String key, String value) {
        if (!Strings.isNullOrEmpty(value)) {
            properties.put(key, value);
        }
    }

    public abstract Map<String, String> getCopiedProperties();

    public static final ShowResultSetMetaData STORAGE_VAULT_META_DATA =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("StorageVaultName", ScalarType.createVarchar(100)))
                .addColumn(new Column("StoragevaultId", ScalarType.createVarchar(20)))
                .addColumn(new Column("Propeties", ScalarType.createVarchar(65535)))
                .build();

    public static List<String> convertToShowStorageVaultProperties(Cloud.ObjectStoreInfoPB info) {
        Cloud.ObjectStoreInfoPB.Builder builder = Cloud.ObjectStoreInfoPB.newBuilder();
        builder.mergeFrom(info);
        List<String> row = new ArrayList<>();
        row.add(info.getName());
        row.add(info.getId());
        TextFormat.Printer printer = TextFormat.printer();
        builder.clearId();
        builder.clearName();
        builder.setSk("xxxxxxx");
        row.add(printer.shortDebugString(builder));
        return row;
    }

    public static List<String> convertToShowStorageVaultProperties(Cloud.StorageVaultPB vault) {
        List<String> row = new ArrayList<>();
        row.add(vault.getName());
        row.add(vault.getId());
        Cloud.HdfsVaultInfo.Builder builder = Cloud.HdfsVaultInfo.newBuilder();
        builder.mergeFrom(vault.getHdfsInfo());
        TextFormat.Printer printer = TextFormat.printer();
        row.add(printer.shortDebugString(builder));
        return row;
    }
}
