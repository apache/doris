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

import org.apache.doris.analysis.AddRemoteStorageClause;
import org.apache.doris.analysis.DropRemoteStorageClause;
import org.apache.doris.analysis.ModifyRemoteStorageClause;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.util.PrintableMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

public class RemoteStorageMgr {
    private static final Logger LOG = LogManager.getLogger(RemoteStorageMgr.class);

    public static final ImmutableList<String> REMOTE_STORAGE_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("Type").add("Properties")
            .build();
    private final Map<String, RemoteStorageInfo> storageInfoMap = Maps.newHashMap();
    private final ReentrantLock lock = new ReentrantLock();
    private RemoteStorageProcNode procNode = null;

    public RemoteStorageMgr() {

    }

    public Map<String, RemoteStorageInfo> getStorageInfoMap() {
        return storageInfoMap;
    }

    public RemoteStorageProperty getRemoteStorageByName(String storageName) throws AnalysisException {
        RemoteStorageInfo info = storageInfoMap.get(storageName);
        if (info == null) {
            throw new AnalysisException("Unknown remote storage name: " + storageName);
        }
        return info.getRemoteStorageProperty();
    }

    public void addRemoteStorage(AddRemoteStorageClause clause) throws DdlException {
        lock.lock();
        try {
            String storageName = clause.getStorageName();
            if (storageInfoMap.containsKey(storageName)) {
                throw new DdlException("Remote storage[" + storageName + "] has already in remote storages.");
            }
            Map<String, String> properties = clause.getProperties();
            RemoteStorageProperty.RemoteStorageType storageType = clause.getRemoteStorageType();
            RemoteStorageProperty storageProperty;
            switch (storageType) {
                case S3:
                    storageProperty = new S3Property(properties);
                    break;
                default:
                    throw new DdlException("Unknown remote storage type: " + storageType.name());
            }
            RemoteStorageInfo storageInfo = new RemoteStorageInfo(storageName, storageProperty);
            Catalog.getCurrentCatalog().getEditLog().logAddRemoteStorage(storageInfo);
            storageInfoMap.put(storageName, storageInfo);
        } finally {
            lock.unlock();
        }
    }

    public void replayAddRemoteStorage(RemoteStorageInfo storageInfo) {
        lock.lock();
        try {
            String storageName = storageInfo.getRemoteStorageName();
            RemoteStorageInfo info = storageInfoMap.get(storageName);
            if (info == null) {
                info = storageInfo;
            }
            storageInfoMap.put(storageName, info);
        } finally {
            lock.unlock();
        }
    }

    public void dropRemoteStorage(DropRemoteStorageClause clause) throws DdlException {
        lock.lock();
        try {
            String storageName = clause.getStorageName();
            RemoteStorageInfo storageInfo = storageInfoMap.get(storageName);
            if (storageInfo == null) {
                throw new DdlException("Unknown remote storage name: " + storageName);
            }

            // Check table using the remote storage before dropping
            List<String> usedTables = new ArrayList<>();
            List<Long> dbIds = Catalog.getCurrentCatalog().getDbIds();
            for (Long dbId : dbIds) {
                Optional<Database> database = Catalog.getCurrentCatalog().getDb(dbId);
                database.ifPresent(db -> {
                    List<Table> tables = db.getTablesOnIdOrder();
                    for (Table table : tables) {
                        if (table instanceof OlapTable) {
                            PartitionInfo partitionInfo = ((OlapTable) table).getPartitionInfo();
                            List<Long> partitionIds = ((OlapTable) table).getPartitionIds();
                            for (Long partitionId : partitionIds) {
                                DataProperty dataProperty = partitionInfo.getDataProperty(partitionId);
                                if (storageName.equals(dataProperty.getRemoteStorageName())) {
                                    usedTables.add(db.getFullName() + "." + table.getName());
                                }
                            }
                        }
                    }
                });
            }
            if (usedTables.size() > 0) {
                LOG.warn("Can not drop remote storage, since it's used in tables {}", usedTables);
                throw new DdlException("Can not drop remote storage, since it's used in tables " + usedTables);
            }

            Catalog.getCurrentCatalog().getEditLog().logDropRemoteStorage(storageInfo);
            storageInfoMap.remove(storageName);
        } finally {
            lock.unlock();
        }
    }

    public void replayDropRemoteStorage(RemoteStorageInfo info) {
        lock.lock();
        try {
            storageInfoMap.remove(info.getRemoteStorageName());
        } finally {
            lock.unlock();
        }
    }

    public void modifyRemoteStorage(ModifyRemoteStorageClause clause) throws DdlException {
        lock.lock();
        try {
            String storageName = clause.getStorageName();
            RemoteStorageInfo storageInfo = storageInfoMap.get(storageName);
            if (storageInfo == null) {
                throw new DdlException("Unknown remote storage name: " + storageName);
            }
            storageInfo.getRemoteStorageProperty().modifyRemoteStorage(clause.getProperties());
            Catalog.getCurrentCatalog().getEditLog().logModifyRemoteStorage(storageInfo);
            storageInfoMap.put(storageName, storageInfo);
        } finally {
            lock.unlock();
        }
    }

    public void replayModifyRemoteStorage(RemoteStorageInfo storageInfo) {
        lock.lock();
        try {
            String storageName = storageInfo.getRemoteStorageName();
            storageInfoMap.put(storageName, storageInfo);
        } finally {
            lock.unlock();
        }
    }

    public List<List<String>> getRemoteStoragesInfo() {
        lock.lock();
        try {
            if (procNode == null) {
                procNode = new RemoteStorageProcNode();
            }
            return procNode.fetchResult().getRows();
        } finally {
            lock.unlock();
        }
    }

    public RemoteStorageProcNode getProcNode() {
        lock.lock();
        try {
            if (procNode == null) {
                procNode = new RemoteStorageProcNode();
            }
            return procNode;
        } finally {
            lock.unlock();
        }
    }

    public class RemoteStorageProcNode implements ProcNodeInterface {

        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(REMOTE_STORAGE_PROC_NODE_TITLE_NAMES);

            lock.lock();
            try {
                for (Map.Entry<String, RemoteStorageInfo> entry : storageInfoMap.entrySet()) {
                    String storageName = entry.getKey();
                    RemoteStorageProperty property = entry.getValue().getRemoteStorageProperty();
                    String storageType = property.getStorageType().name();
                    Map<String, String> properties = property.getProperties();

                    List<String> row = Lists.newArrayList();
                    row.add(storageName);
                    row.add(storageType);
                    StringBuilder sb = new StringBuilder();
                    sb.append(new PrintableMap<String, String>(properties, " = ", true, true, true));
                    row.add(sb.toString());
                    result.addRow(row);
                }
            } finally {
                lock.unlock();
            }

            return result;
        }
    }

    public static class RemoteStorageInfo implements Writable {
        private String remoteStorageName;
        private RemoteStorageProperty remoteStorageProperty;

        public RemoteStorageInfo() {

        }

        public RemoteStorageInfo(String remoteStorageName, RemoteStorageProperty storageProperty) {
            this.remoteStorageName = remoteStorageName;
            this.remoteStorageProperty = storageProperty;
        }

        public String getRemoteStorageName() {
            return remoteStorageName;
        }

        public RemoteStorageProperty getRemoteStorageProperty() {
            return remoteStorageProperty;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, remoteStorageName);
            RemoteStorageProperty.writeTo(remoteStorageProperty, out);
        }

        public void readFields(DataInput in) throws IOException {
            remoteStorageName = Text.readString(in);
            remoteStorageProperty = RemoteStorageProperty.readIn(in);
        }

        public static RemoteStorageInfo readIn(DataInput in) throws IOException {
            RemoteStorageInfo info = new RemoteStorageInfo();
            info.readFields(in);
            return info;
        }
    }
}
