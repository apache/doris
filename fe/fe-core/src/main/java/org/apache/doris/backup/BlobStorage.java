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

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.service.FrontendOptions;

import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class BlobStorage implements Writable {

    public static final String STORAGE_TYPE = "_DORIS_STORAGE_TYPE_";
    private Map<String, String> properties = Maps.newHashMap();
    private String name;
    private StorageBackend.StorageType type;
    private String location;

    public static String clientId() {
        return FrontendOptions.getLocalHostAddress() + ":" + Config.edit_log_port;
    }

    public static BlobStorage create(String name, StorageBackend.StorageType type, Map<String, String> properties) {
        if (type == StorageBackend.StorageType.S3) {
            return new S3Storage(properties);
        } else if (type == StorageBackend.StorageType.BROKER) {
            return new BrokerStorage(name, properties);
        } else {
            throw new UnsupportedOperationException(type.toString() + "backend is not implemented");
        }
    }

    public static BlobStorage read(DataInput in) throws IOException {
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
        return BlobStorage.create(name, type, properties);
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public StorageBackend.StorageType getType() {
        return type;
    }

    public void setType(StorageBackend.StorageType type) {
        this.type = type;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public abstract Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize);

    // directly upload the content to remote file
    public abstract Status directUpload(String content, String remoteFile);

    public abstract Status upload(String localPath, String remotePath);

    public abstract Status rename(String origFilePath, String destFilePath);

    public abstract Status delete(String remotePath);

    // List files in remotePath
    // The remote file name will only contains file name only(Not full path)
    public abstract Status list(String remotePath, List<RemoteFile> result);

    public abstract Status makeDir(String remotePath);

    public abstract Status checkPathExist(String remotePath);

    public abstract StorageBackend.StorageType getStorageType();

    @Override
    public void write(DataOutput out) throws IOException {
        // must write type first
        Text.writeString(out, name);
        properties.put(STORAGE_TYPE, type.name());
        out.writeInt(getProperties().size());
        for (Map.Entry<String, String> entry : getProperties().entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }
}
