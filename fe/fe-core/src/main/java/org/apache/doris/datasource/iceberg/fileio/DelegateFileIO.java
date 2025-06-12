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

package org.apache.doris.datasource.iceberg.fileio;

import org.apache.doris.backup.Status;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.FileSystem;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.io.DorisPath;

import com.google.common.collect.Iterables;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DelegateFileIO implements SupportsBulkOperations {
    private static final int DELETE_BATCH_SIZE = 1000;
    private Map<String, String> properties;
    private FileSystem fileSystem;

    public DelegateFileIO() {

    }

    public DelegateFileIO(FileSystem fileSystem) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem is null");
    }

    @Override
    public InputFile newInputFile(String path) {
        return new DelegateInputFile(fileSystem.newInputFile(new DorisPath(path)));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        return new DelegateInputFile(fileSystem.newInputFile(new DorisPath(path), length));
    }

    @Override
    public OutputFile newOutputFile(String path) {
        return new DelegateOutputFile(fileSystem, new DorisPath(path));
    }

    @Override
    public void deleteFile(String path) {
        Status status = fileSystem.delete(path);
        if (!status.ok()) {
            throw new UncheckedIOException(
                    new IOException("Failed to delete file: " + path + ", " + status.toString()));
        }
    }

    @Override
    public void deleteFile(InputFile file) {
        SupportsBulkOperations.super.deleteFile(file);
    }

    @Override
    public void deleteFile(OutputFile file) {
        SupportsBulkOperations.super.deleteFile(file);
    }

    @Override
    public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
        Iterable<List<String>> partitions = Iterables.partition(pathsToDelete, DELETE_BATCH_SIZE);
        partitions.forEach(this::deleteBatch);
    }

    private void deleteBatch(List<String> filesToDelete) {
        Status status = fileSystem.deleteAll(filesToDelete);
        if (!status.ok()) {
            throw new UncheckedIOException(new IOException("Failed to delete some or all files: " + status.toString()));
        }
    }

    @Override
    public InputFile newInputFile(ManifestFile manifest) {
        return SupportsBulkOperations.super.newInputFile(manifest);
    }

    @Override
    public InputFile newInputFile(DataFile file) {
        return SupportsBulkOperations.super.newInputFile(file);
    }

    @Override
    public InputFile newInputFile(DeleteFile file) {
        return SupportsBulkOperations.super.newInputFile(file);
    }

    @Override
    public Map<String, String> properties() {
        return properties;
    }

    @Override
    public void initialize(Map<String, String> properties) {
        StorageProperties storageProperties = StorageProperties.createPrimary(properties);
        this.fileSystem = FileSystemFactory.get(storageProperties);
        this.properties = properties;
    }

    @Override
    public void close() {
    }
}
