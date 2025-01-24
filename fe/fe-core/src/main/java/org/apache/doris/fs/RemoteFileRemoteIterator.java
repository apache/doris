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

import org.apache.doris.fs.remote.RemoteFile;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public class RemoteFileRemoteIterator
        implements RemoteIterator<RemoteFile> {
    private final List<RemoteFile> remoteFileList;
    private int currentIndex = 0;

    public RemoteFileRemoteIterator(List<RemoteFile> remoteFileList) {
        this.remoteFileList = Objects.requireNonNull(remoteFileList, "iterator is null");
    }

    @Override
    public boolean hasNext() throws FileSystemIOException {
        return currentIndex < remoteFileList.size();
    }

    @Override
    public RemoteFile next() throws FileSystemIOException {
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements in RemoteFileRemoteIterator");
        }
        return remoteFileList.get(currentIndex++);
    }
}
