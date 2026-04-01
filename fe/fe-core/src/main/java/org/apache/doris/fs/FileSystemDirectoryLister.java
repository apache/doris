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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemIOException;
import org.apache.doris.filesystem.FileSystemTransferUtil;
import org.apache.doris.filesystem.RemoteIterator;
import org.apache.doris.filesystem.SimpleRemoteIterator;

import java.io.IOException;
import java.util.List;

public class FileSystemDirectoryLister implements DirectoryLister {
    public RemoteIterator<FileEntry> listFiles(FileSystem fs, boolean recursive,
            TableIf table, String location)
            throws FileSystemIOException {
        try {
            List<FileEntry> entries = FileSystemTransferUtil.globList(fs, location, recursive);
            return new SimpleRemoteIterator(entries.iterator());
        } catch (IOException ex) {
            throw new FileSystemIOException(ex.getMessage(), ex);
        }
    }
}
