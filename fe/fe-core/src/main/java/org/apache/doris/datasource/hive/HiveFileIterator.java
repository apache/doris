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

package org.apache.doris.datasource.hive;

import org.apache.doris.datasource.hive.HiveExternalMetaCache.FileCacheValue;
import org.apache.doris.datasource.hive.HiveExternalMetaCache.HiveFileStatus;
import org.apache.doris.fs.RemoteIterator;

import java.util.Collections;
import java.util.Iterator;

/** One partition's files and split metadata, without requiring a materialized file list. */
public interface HiveFileIterator extends RemoteIterator<HiveFileStatus> {
    FileCacheValue metadata();

    static HiveFileIterator of(FileCacheValue value) {
        return new HiveFileIterator() {
            private Iterator<HiveFileStatus> files = value.getFiles().iterator();

            @Override
            public FileCacheValue metadata() {
                return value;
            }

            @Override
            public boolean hasNext() {
                return files.hasNext();
            }

            @Override
            public HiveFileStatus next() {
                return files.next();
            }

            @Override
            public void close() {
                files = Collections.emptyIterator();
            }
        };
    }
}
