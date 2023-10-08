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

import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.CacheBulkLoader;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.fs.remote.RemoteFileSystem;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.mapred.JobConf;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class FileSystemCache {

    private LoadingCache<FileSystemCacheKey, RemoteFileSystem> fileSystemCache;

    public FileSystemCache(ExecutorService executor) {
        fileSystemCache = CacheBuilder.newBuilder().maximumSize(Config.max_remote_file_system_cache_num)
            .expireAfterAccess(Config.external_cache_expire_time_minutes_after_access, TimeUnit.MINUTES)
            .build(new CacheBulkLoader<FileSystemCacheKey, RemoteFileSystem>() {
                @Override
                protected ExecutorService getExecutor() {
                    return executor;
                }

                @Override
                public RemoteFileSystem load(FileSystemCacheKey key) {
                    return loadFileSystem(key);
                }
            });
    }

    private RemoteFileSystem loadFileSystem(FileSystemCacheKey key) {
        return FileSystemFactory.getByType(key.type, key.conf);
    }

    public RemoteFileSystem getRemoteFileSystem(FileSystemCacheKey key) {
        try {
            return fileSystemCache.get(key);
        } catch (ExecutionException e) {
            throw new CacheException("failed to get remote filesystem for type[%s]", e, key.type);
        }
    }

    public static class FileSystemCacheKey {
        private final FileSystemType type;
        // eg: hdfs://nameservices1
        private final String fsIdent;
        private final JobConf conf;

        public FileSystemCacheKey(Pair<FileSystemType, String> fs, JobConf conf) {
            this.type = fs.first;
            this.fsIdent = fs.second;
            this.conf = conf;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof FileSystemCacheKey)) {
                return false;
            }
            return type.equals(((FileSystemCacheKey) obj).type)
                    && fsIdent.equals(((FileSystemCacheKey) obj).fsIdent)
                    && conf == ((FileSystemCacheKey) obj).conf;
        }

        @Override
        public int hashCode() {
            return Objects.hash(conf, fsIdent, type);
        }
    }
}
