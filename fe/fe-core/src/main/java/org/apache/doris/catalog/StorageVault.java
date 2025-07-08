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

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateStorageVaultCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.TextFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;

public abstract class StorageVault {
    public static final String REFERENCE_SPLIT = "@";
    public static final String INCLUDE_DATABASE_LIST = "include_database_list";
    public static final String EXCLUDE_DATABASE_LIST = "exclude_database_list";
    public static final String LOWER_CASE_META_NAMES = "lower_case_meta_names";
    public static final String META_NAMES_MAPPING = "meta_names_mapping";

    public static class PropertyKey {
        public static final String VAULT_NAME = "VAULT_NAME"; // used when changing storage vault name
        public static final String TYPE = "type";
    }

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
    private int pathVersion = 0;
    private int numShard = 0;

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

    public static StorageVault fromCommand(CreateStorageVaultCommand command) throws DdlException, UserException {
        return getStorageVaultInstanceByCommand(command);
    }

    public boolean ifNotExists() {
        return this.ifNotExists;
    }

    public boolean setAsDefault() {
        return this.setAsDefault;
    }

    public int getPathVersion() {
        return pathVersion;
    }

    public int getNumShard() {
        return numShard;
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
            getStorageVaultInstanceByCommand(CreateStorageVaultCommand command) throws DdlException, UserException {
        StorageVaultType type = command.getVaultType();
        String name = command.getVaultName();
        boolean ifNotExists = command.isIfNotExists();
        boolean setAsDefault = command.isSetAsDefault();
        StorageVault vault;
        switch (type) {
            case HDFS:
                vault = new HdfsStorageVault(name, ifNotExists, setAsDefault);
                vault.modifyProperties(command.getProperties());
                break;
            case S3:
                CreateResourceInfo info = new CreateResourceInfo(false, ifNotExists, name, command.getProperties());
                CreateResourceCommand resourceCommand =
                        new CreateResourceCommand(info);
                resourceCommand.getInfo().analyzeResourceType();
                vault = new S3StorageVault(name, ifNotExists, setAsDefault, resourceCommand);
                break;
            default:
                throw new DdlException("Unknown StorageVault type: " + type);
        }
        vault.checkCreationProperties(command.getProperties());
        vault.pathVersion = command.getPathVersion();
        vault.numShard = command.getNumShard();
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
    public abstract void modifyProperties(ImmutableMap<String, String> properties) throws DdlException;

    /**
     * Check properties in child resources
     * @param properties
     * @throws UserException
     */
    public void checkCreationProperties(Map<String, String> properties) throws UserException {
        String type = null;
        for (Map.Entry<String, String> property : properties.entrySet()) {
            if (property.getKey().equalsIgnoreCase(StorageVault.PropertyKey.TYPE)) {
                type = property.getValue();
            }
        }

        Preconditions.checkArgument(type != null, "Missing property " + PropertyKey.TYPE);
        Preconditions.checkArgument(!type.isEmpty(), "Property " + PropertyKey.TYPE + " cannot be empty");
    }

    protected void replaceIfEffectiveValue(Map<String, String> properties, String key, String value) {
        if (!Strings.isNullOrEmpty(value)) {
            properties.put(key, value);
        }
    }

    public abstract Map<String, String> getCopiedProperties();

    public static final ShowResultSetMetaData STORAGE_VAULT_META_DATA =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", ScalarType.createVarchar(100)))
                .addColumn(new Column("Id", ScalarType.createVarchar(20)))
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

            if (vault.getObjInfo().hasAk()) {
                builder.setSk("xxxxxxx");
            }

            if (!vault.getObjInfo().hasUsePathStyle()) {
                // There is no `use_path_style` field in old version, think `use_path_style` false
                builder.setUsePathStyle(false);
            }
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
                .filter(i -> columns.get(i).getName().equals("Id"))
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
