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

import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.fs.remote.RemoteFileSystem;

import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

public class FileSystemCache {

    private final LoadingCache<FileSystemCacheKey, RemoteFileSystem> fileSystemCache;

    public FileSystemCache() {
        // no need to set refreshAfterWrite, because the FileSystem is created once and never changed
        CacheFactory fsCacheFactory = new CacheFactory(
                OptionalLong.of(86400L),
                OptionalLong.empty(),
                Config.max_remote_file_system_cache_num,
                false,
                null);
        fileSystemCache = fsCacheFactory.buildCache(this::loadFileSystem);
    }

    private RemoteFileSystem loadFileSystem(FileSystemCacheKey key) {
        return FileSystemFactory.getRemoteFileSystem(key.type, key.getFsProperties(), key.bindBrokerName);
    }

    public RemoteFileSystem getRemoteFileSystem(FileSystemCacheKey key) {
        return fileSystemCache.get(key);
    }

    public static class FileSystemCacheKey {
        private final FileSystemType type;
        // eg: hdfs://nameservices1
        private final String fsIdent;
        private final Map<String, String> properties;
        private final String bindBrokerName;
        // only for creating new file system
        private final Configuration conf;

        public FileSystemCacheKey(Pair<FileSystemType, String> fs,
                Map<String, String> properties,
                String bindBrokerName,
                Configuration conf) {
            this.type = fs.first;
            this.fsIdent = fs.second;
            this.properties = properties;
            this.bindBrokerName = bindBrokerName;
            this.conf = conf;
        }

        public FileSystemCacheKey(Pair<FileSystemType, String> fs,
                Map<String, String> properties, String bindBrokerName) {
            this(fs, properties, bindBrokerName, null);
        }

        public Map<String, String> getFsProperties() {
            if (conf == null) {
                return properties;
            }
            Map<String, String> result = new HashMap<>();
            conf.iterator().forEachRemaining(e -> result.put(e.getKey(), e.getValue()));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof FileSystemCacheKey)) {
                return false;
            }
            FileSystemCacheKey o = (FileSystemCacheKey) obj;
            boolean equalsWithoutBroker = type.equals(o.type)
                    && fsIdent.equals(o.fsIdent)
                    && properties.equals(o.properties);
            if (bindBrokerName == null) {
                return equalsWithoutBroker && o.bindBrokerName == null;
            }
            return equalsWithoutBroker && bindBrokerName.equals(o.bindBrokerName);
        }

        @Override
        public int hashCode() {
            if (bindBrokerName == null) {
                return Objects.hash(properties, fsIdent, type);
            }
            return Objects.hash(properties, fsIdent, type, bindBrokerName);
        }
    }
}
