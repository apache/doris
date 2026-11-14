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


package org.apache.doris.connector.deltalake;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Filesystem catalogs expose exactly warehouse/database/table/_delta_log. */
final class DeltaLakeFileSystemTableResolver implements DeltaLakeTableResolver {
    private final Path warehouse;
    private final DeltaLakeFileIO fileIO;

    DeltaLakeFileSystemTableResolver(String warehouse, DeltaLakeFileIO fileIO) {
        this.warehouse = new Path(warehouse);
        this.fileIO = fileIO;
    }

    @Override
    public List<String> listDatabases() throws IOException {
        List<String> databases = new ArrayList<>();
        for (FileStatus child : children(warehouse)) {
            if (child.isDirectory()) {
                databases.add(child.getPath().getName());
            }
        }
        Collections.sort(databases);
        return databases;
    }

    @Override
    public List<String> listTables(String database) throws IOException {
        Path databasePath = child(warehouse, database);
        List<String> tables = new ArrayList<>();
        for (FileStatus entry : children(databasePath)) {
            if (entry.isDirectory() && hasDeltaLog(entry.getPath())) {
                tables.add(entry.getPath().getName());
            }
        }
        Collections.sort(tables);
        return tables;
    }

    @Override
    public Optional<String> tableLocation(String database, String table) throws IOException {
        Path path = child(child(warehouse, database), table);
        return hasDeltaLog(path) ? Optional.of(fileIO.resolvePath(path.toString()))
                : Optional.empty();
    }

    private FileStatus[] children(Path path) throws IOException {
        try {
            return fileIO.fileSystem(path).listStatus(path);
        } catch (FileNotFoundException e) {
            // A new warehouse or an absent database is an empty namespace, not an IO failure.
            return new FileStatus[0];
        }
    }

    private boolean hasDeltaLog(Path table) throws IOException {
        Path log = new Path(table, "_delta_log");
        FileSystem fileSystem = fileIO.fileSystem(log);
        try {
            return fileSystem.getFileStatus(log).isDirectory();
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    private static Path child(Path parent, String name) {
        // Hadoop Path treats an absolute child or URI as a replacement for the warehouse.
        if (name.isEmpty() || ".".equals(name) || "..".equals(name)
                || name.indexOf('/') >= 0 || name.indexOf('\\') >= 0 || name.indexOf(':') >= 0) {
            throw new IllegalArgumentException("Delta filesystem database/table must be a single path name: " + name);
        }
        return new Path(parent, name);
    }
}
