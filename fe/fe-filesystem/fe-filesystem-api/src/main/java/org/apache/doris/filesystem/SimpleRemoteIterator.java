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
// This file is copied from
// https://github.com/trinodb/trino/blob/438/plugin/trino-hive/src/main/java/io/trino/plugin/hive/fs/SimpleRemoteIterator.java
// and modified by Doris

package org.apache.doris.filesystem;

import java.util.Iterator;
import java.util.Objects;

public class SimpleRemoteIterator implements RemoteIterator<FileEntry> {
    private final Iterator<FileEntry> iterator;

    public SimpleRemoteIterator(Iterator<FileEntry> iterator) {
        this.iterator = Objects.requireNonNull(iterator, "iterator is null");
    }

    @Override
    public boolean hasNext() throws FileSystemIOException {
        return iterator.hasNext();
    }

    @Override
    public FileEntry next() throws FileSystemIOException {
        return iterator.next();
    }
}
