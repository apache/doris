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

package org.apache.doris.backup;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Writable;
import org.apache.doris.meta.MetaContext;

import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BackupMeta implements Writable {

    // tbl name -> tbl
    private Map<String, Table> tblNameMap = Maps.newHashMap();
    // tbl id -> tbl
    private Map<Long, Table> tblIdMap = Maps.newHashMap();
    // resource name -> resource
    private Map<String, Resource> resourceNameMap = Maps.newHashMap();

    private BackupMeta() {
    }

    public BackupMeta(List<Table> tables, List<Resource> resources) {
        for (Table table : tables) {
            tblNameMap.put(table.getName(), table);
            tblIdMap.put(table.getId(), table);
        }
        for (Resource resource : resources) {
            resourceNameMap.put(resource.getName(), resource);
        }
    }

    public Map<String, Table> getTables() {
        return tblNameMap;
    }

    public Map<String, Resource> getResourceNameMap() {
        return resourceNameMap;
    }

    public Table getTable(String tblName) {
        return tblNameMap.get(tblName);
    }

    public Resource getResource(String resourceName) {
        return resourceNameMap.get(resourceName);
    }

    public Table getTable(Long tblId) {
        return tblIdMap.get(tblId);
    }

    public static BackupMeta fromFile(String filePath, int metaVersion) throws IOException {
        File file = new File(filePath);
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(metaVersion);
        metaContext.setThreadLocalInfo();
        try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
            BackupMeta backupMeta = BackupMeta.read(dis);
            return backupMeta;
        } finally {
            MetaContext.remove();
        }
    }

    public void writeToFile(File metaInfoFile) throws IOException {
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(metaInfoFile));
        try {
            write(dos);
            dos.flush();
        } finally {
            dos.close();
        }
    }

    public boolean compatibleWith(BackupMeta other) {
        // TODO
        return false;
    }

    public static BackupMeta read(DataInput in) throws IOException {
        BackupMeta backupMeta = new BackupMeta();
        backupMeta.readFields(in);
        return backupMeta;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(tblNameMap.size());
        for (Table table : tblNameMap.values()) {
            table.write(out);
        }
        out.writeInt(resourceNameMap.size());
        for (Resource resource : resourceNameMap.values()) {
            resource.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Table tbl = Table.read(in);
            tblNameMap.put(tbl.getName(), tbl);
            tblIdMap.put(tbl.getId(), tbl);
        }
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_95) {
            return;
        }
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            Resource resource = Resource.read(in);
            resourceNameMap.put(resource.getName(), resource);
        }
    }
}
