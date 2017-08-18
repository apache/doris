// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.broker.bos;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

/**
 * <p>
 * An abstraction for a key-based {@link File} store.
 * </p>
 */
interface NativeFileSystemStore {
  
    void initialize(URI uri, Configuration conf) throws IOException;

    void storeFile(String key, File file) throws IOException;
    void storeEmptyFile(String key) throws IOException;

    OutputStream createFile(String key, Configuration conf, int bufferSize) throws IOException;

    FileMetadata retrieveMetadata(String key) throws IOException;
    InputStream retrieve(String key) throws IOException;
    InputStream retrieve(String key, long byteRangeStart) throws IOException;

    PartialListing list(String prefix, int maxListingLength) throws IOException;
    PartialListing list(String prefix, int maxListingLength, String priorLastKey) throws IOException;
    PartialListing list(String prefix, int maxListingLength, String priorLastKey, String delimiter) throws IOException;

    void delete(String key) throws IOException;
    void copy(String srcKey, String dstKey) throws IOException;
    void rename(String srcKey, String dstKey) throws IOException;
    /**
     * Delete all keys with the given prefix. Used for testing.
     * @throws IOException
     */
    void purge(String prefix) throws IOException;

    /**
     * Diagnostic method to dump state to the console.
     * @throws IOException
     */
    void dump() throws IOException;
}