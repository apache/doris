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

package org.apache.doris.avro;

import com.google.common.base.Objects;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class AvroFileCache {
    private static final Logger LOG = LoggerFactory.getLogger(AvroFileCache.class);
    private static final Map<AvroFileCacheKey, AvroFileMeta> fileCache = Maps.newHashMap();

    public static void addFileMeta(AvroFileCacheKey avroFileCacheKey, AvroFileMeta avroFileMeta) {
        fileCache.put(avroFileCacheKey, avroFileMeta);
    }

    public static AvroFileMeta getAvroFileMeta(AvroFileCacheKey key) {
        return fileCache.get(key);
    }

    public static void invalidateFileCache(AvroFileCacheKey key) {
        fileCache.remove(key);
    }

    public static class AvroFileMeta {
        private final String schema;
        private Set<String> requiredFields;
        private Long splitStartOffset;
        private Long splitSize;

        AvroFileMeta(String schema) {
            this.schema = schema;
        }

        public String getSchema() {
            return schema;
        }

        public void setRequiredFields(Set<String> requiredFields) {
            this.requiredFields = requiredFields;
        }

        public void setSplitStartOffset(Long splitStartOffset) {
            this.splitStartOffset = splitStartOffset;
        }

        public void setSplitSize(Long splitSize) {
            this.splitSize = splitSize;
        }

        public Long getSplitStartOffset() {
            return this.splitStartOffset;
        }

        public Long getSplitSize() {
            return this.splitSize;
        }

        public Set<String> getRequiredFields() {
            return requiredFields;
        }
    }

    protected static class AvroFileCacheKey {
        private final String fileType;
        private final String uri;

        AvroFileCacheKey(String fileType, String uri) {
            this.fileType = fileType;
            this.uri = uri;
        }

        protected String getUri() {
            return uri;
        }

        protected String getFileType() {
            return fileType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AvroFileCacheKey that = (AvroFileCacheKey) o;
            return Objects.equal(fileType, that.fileType) && Objects.equal(uri, that.uri);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(fileType, uri);
        }

        @Override
        public String toString() {
            return "AvroFileCacheKey{"
                    + "fileType='" + fileType + '\''
                    + ", uri='" + uri + '\''
                    + '}';
        }
    }
}
