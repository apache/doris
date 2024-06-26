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

import org.apache.doris.analysis.CreateResourceStmt;
import org.apache.doris.analysis.CreateStorageVaultStmt;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.protobuf.TextFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;

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
    private boolean setAsDefault;

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

    public StorageVault(String name, StorageVaultType type, boolean ifNotExists, boolean setAsDefault) {
        this.name = name;
        this.type = type;
        this.ifNotExists = ifNotExists;
        this.setAsDefault = setAsDefault;
    }

    public static StorageVault fromStmt(CreateStorageVaultStmt stmt) throws DdlException, UserException {
        return getStorageVaultInstance(stmt);
    }

    public boolean ifNotExists() {
        return this.ifNotExists;
    }

    public boolean setAsDefault() {
        return this.setAsDefault;
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
    private static StorageVault
            getStorageVaultInstance(CreateStorageVaultStmt stmt) throws DdlException, UserException {
        StorageVaultType type = stmt.getStorageVaultType();
        String name = stmt.getStorageVaultName();
        boolean ifNotExists = stmt.isIfNotExists();
        boolean setAsDefault = stmt.setAsDefault();
        StorageVault vault;
        switch (type) {
            case HDFS:
                vault = new HdfsStorageVault(name, ifNotExists, setAsDefault);
                vault.modifyProperties(stmt.getProperties());
                break;
            case S3:
                CreateResourceStmt resourceStmt =
                        new CreateResourceStmt(false, ifNotExists, name, stmt.getProperties());
                resourceStmt.analyzeResourceType();
                vault = new S3StorageVault(name, ifNotExists, setAsDefault, resourceStmt);
                break;
            default:
                throw new DdlException("Unknown StorageVault type: " + type);
        }
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
                .addColumn(new Column("StorageVaultId", ScalarType.createVarchar(20)))
                .addColumn(new Column("Propeties", ScalarType.createVarchar(65535)))
                .addColumn(new Column("IsDefault", ScalarType.createVarchar(5)))
                .build();

    public static List<String> convertToShowStorageVaultProperties(Cloud.StorageVaultPB vault) {
        List<String> row = new ArrayList<>();
        row.add(vault.getName());
        row.add(vault.getId());
        TextFormat.Printer printer = TextFormat.printer();
        if (vault.hasHdfsInfo()) {
            Cloud.HdfsVaultInfo.Builder builder = Cloud.HdfsVaultInfo.newBuilder();
            builder.mergeFrom(vault.getHdfsInfo());
            row.add(printer.shortDebugString(builder));
        }
        if (vault.hasObjInfo()) {
            Cloud.ObjectStoreInfoPB.Builder builder = Cloud.ObjectStoreInfoPB.newBuilder();
            builder.mergeFrom(vault.getObjInfo());
            builder.clearId();
            builder.setSk("xxxxxxx");
            row.add(printer.shortDebugString(builder));
        }
        row.add("false");
        return row;
    }

    public static void setDefaultVaultToShowVaultResult(List<List<String>> rows, String vaultId) {
        List<Column> columns = STORAGE_VAULT_META_DATA.getColumns();

        int isDefaultIndex = IntStream.range(0, columns.size())
                .filter(i -> columns.get(i).getName().equals("IsDefault"))
                .findFirst()
                .orElse(-1);

        if (isDefaultIndex == -1) {
            return;
        }

        int vaultIdIndex = IntStream.range(0, columns.size())
                .filter(i -> columns.get(i).getName().equals("StorageVaultId"))
                .findFirst()
                .orElse(-1);

        if (vaultIdIndex == -1) {
            return;
        }

        for (int cnt = 0; cnt < rows.size(); cnt++) {
            if (rows.get(cnt).get(vaultIdIndex).equals(vaultId)) {
                List<String> defaultVaultRow = rows.get(cnt);
                defaultVaultRow.set(isDefaultIndex, "true");
            }
        }
    }
}
