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

package org.apache.doris.fs;

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.fs.remote.RemoteFileSystem;

import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Use for persistence, Repository will persist properties of file system.
 */
public abstract class PersistentFileSystem implements FileSystem, Writable {
    public static final String STORAGE_TYPE = "_DORIS_STORAGE_TYPE_";
    protected Map<String, String> properties = Maps.newHashMap();
    protected String name;
    protected StorageBackend.StorageType type;

    public boolean needFullPath() {
        return type == StorageBackend.StorageType.S3
                    || type == StorageBackend.StorageType.OFS
                    || type == StorageBackend.StorageType.JFS;
    }

    public PersistentFileSystem(String name, StorageBackend.StorageType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public StorageBackend.StorageType getStorageType() {
        return type;
    }

    /**
     *
     * @param in persisted data
     * @return file systerm
     */
    public static RemoteFileSystem read(DataInput in) throws IOException {
        String name = Text.readString(in);
        Map<String, String> properties = Maps.newHashMap();
        StorageBackend.StorageType type = StorageBackend.StorageType.BROKER;
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            properties.put(key, value);
        }
        if (properties.containsKey(STORAGE_TYPE)) {
            type = StorageBackend.StorageType.valueOf(properties.get(STORAGE_TYPE));
            properties.remove(STORAGE_TYPE);
        }
        return FileSystemFactory.get(name, type, properties);
    }

    public void write(DataOutput out) throws IOException {
        // must write type first
        Text.writeString(out, name);
        properties.put(STORAGE_TYPE, type.name());
        out.writeInt(properties.size());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }
}
