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

import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

public class FileSystemCache {

    private LoadingCache<FileSystemCacheKey, RemoteFileSystem> fileSystemCache;

    public FileSystemCache() {
        // no need to set refreshAfterWrite, because the FileSystem is created once and never changed
        CacheFactory fsCacheFactory = new CacheFactory(
                OptionalLong.of(86400L),
                OptionalLong.empty(),
                Config.max_remote_file_system_cache_num,
                false,
                null);
        fileSystemCache = fsCacheFactory.buildCache(key -> loadFileSystem(key));
    }

    private RemoteFileSystem loadFileSystem(FileSystemCacheKey key) {
        return FileSystemFactory.getRemoteFileSystem(key.type, key.properties, key.bindBrokerName);
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

        public FileSystemCacheKey(Pair<FileSystemType, String> fs,
                Map<String, String> properties, String bindBrokerName) {
            this.type = fs.first;
            this.fsIdent = fs.second;
            this.properties = properties;
            this.bindBrokerName = bindBrokerName;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof FileSystemCacheKey)) {
                return false;
            }
            boolean equalsWithoutBroker = type.equals(((FileSystemCacheKey) obj).type)
                    && fsIdent.equals(((FileSystemCacheKey) obj).fsIdent)
                    && properties == ((FileSystemCacheKey) obj).properties;
            if (bindBrokerName == null) {
                return equalsWithoutBroker;
            }
            return equalsWithoutBroker && bindBrokerName.equals(((FileSystemCacheKey) obj).bindBrokerName);
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
