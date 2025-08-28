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
import org.apache.doris.fs.io.ParsedPath;

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

/**
 * DelegateFileIO is an implementation of the Iceberg SupportsBulkOperations interface.
 * It delegates file operations (such as input, output, and deletion) to the underlying Doris FileSystem.
 * This class is responsible for bridging Doris file system operations with Iceberg's file IO abstraction.
 */
public class DelegateFileIO implements SupportsBulkOperations {
    private static final int DELETE_BATCH_SIZE = 1000;
    /**
     * Properties used to initialize the file system.
     */
    private Map<String, String> properties;
    /**
     * The underlying Doris file system used for file operations.
     */
    private FileSystem fileSystem;

    /**
     * Default constructor.
     */
    public DelegateFileIO() {

    }

    /**
     * Constructor with a specified FileSystem.
     *
     * @param fileSystem the Doris file system to delegate operations to
     */
    public DelegateFileIO(FileSystem fileSystem) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem is null");
    }

    // ===================== File Creation Methods =====================

    /**
     * Creates a new InputFile for the given path.
     *
     * @param path the file path
     * @return an InputFile instance
     */
    @Override
    public InputFile newInputFile(String path) {
        return new DelegateInputFile(fileSystem.newInputFile(new ParsedPath(path)));
    }

    /**
     * Creates a new InputFile for the given path and length.
     *
     * @param path the file path
     * @param length the file length
     * @return an InputFile instance
     */
    @Override
    public InputFile newInputFile(String path, long length) {
        return new DelegateInputFile(fileSystem.newInputFile(new ParsedPath(path), length));
    }

    /**
     * Creates a new OutputFile for the given path.
     *
     * @param path the file path
     * @return an OutputFile instance
     */
    @Override
    public OutputFile newOutputFile(String path) {
        return new DelegateOutputFile(fileSystem, new ParsedPath(path));
    }

    // ===================== File Deletion Methods =====================

    /**
     * Deletes a file at the specified path.
     * Throws UncheckedIOException if deletion fails.
     *
     * @param path the file path to delete
     */
    @Override
    public void deleteFile(String path) {
        Status status = fileSystem.delete(path);
        if (!status.ok()) {
            throw new UncheckedIOException(
                    new IOException("Failed to delete file: " + path + ", " + status.toString()));
        }
    }

    /**
     * Deletes a file represented by an InputFile.
     * Delegates to the default implementation in SupportsBulkOperations.
     *
     * @param file the InputFile to delete
     */
    @Override
    public void deleteFile(InputFile file) {
        SupportsBulkOperations.super.deleteFile(file);
    }

    /**
     * Deletes a file represented by an OutputFile.
     * Delegates to the default implementation in SupportsBulkOperations.
     *
     * @param file the OutputFile to delete
     */
    @Override
    public void deleteFile(OutputFile file) {
        SupportsBulkOperations.super.deleteFile(file);
    }

    /**
     * Deletes multiple files in batches.
     * Throws BulkDeletionFailureException if any batch fails.
     *
     * @param pathsToDelete iterable of file paths to delete
     * @throws BulkDeletionFailureException if deletion fails for any batch
     */
    @Override
    public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
        Iterable<List<String>> partitions = Iterables.partition(pathsToDelete, DELETE_BATCH_SIZE);
        partitions.forEach(this::deleteBatch);
    }

    /**
     * Helper method to delete a batch of files.
     * Throws UncheckedIOException if deletion fails.
     *
     * @param filesToDelete list of file paths to delete
     */
    private void deleteBatch(List<String> filesToDelete) {
        Status status = fileSystem.deleteAll(filesToDelete);
        if (!status.ok()) {
            throw new UncheckedIOException(new IOException("Failed to delete some or all files: " + status.toString()));
        }
    }

    // ===================== Manifest/Data/Delete File Methods =====================

    /**
     * Creates a new InputFile from a ManifestFile.
     * Delegates to the default implementation in SupportsBulkOperations.
     *
     * @param manifest the ManifestFile
     * @return an InputFile instance
     */
    @Override
    public InputFile newInputFile(ManifestFile manifest) {
        return SupportsBulkOperations.super.newInputFile(manifest);
    }

    /**
     * Creates a new InputFile from a DataFile.
     * Delegates to the default implementation in SupportsBulkOperations.
     *
     * @param file the DataFile
     * @return an InputFile instance
     */
    @Override
    public InputFile newInputFile(DataFile file) {
        return SupportsBulkOperations.super.newInputFile(file);
    }

    /**
     * Creates a new InputFile from a DeleteFile.
     * Delegates to the default implementation in SupportsBulkOperations.
     *
     * @param file the DeleteFile
     * @return an InputFile instance
     */
    @Override
    public InputFile newInputFile(DeleteFile file) {
        return SupportsBulkOperations.super.newInputFile(file);
    }

    // ===================== Properties and Initialization =====================

    /**
     * Returns the properties used to initialize the file system.
     *
     * @return the properties map
     */
    @Override
    public Map<String, String> properties() {
        return properties;
    }

    /**
     * Initializes the file system with the given properties.
     *
     * @param properties the properties map
     */
    @Override
    public void initialize(Map<String, String> properties) {
        StorageProperties storageProperties = StorageProperties.createPrimary(properties);
        this.fileSystem = FileSystemFactory.get(storageProperties);
        this.properties = properties;
    }

    /**
     * Closes the file IO and releases any resources if necessary.
     * No-op in this implementation.
     */
    @Override
    public void close() {
    }
}
