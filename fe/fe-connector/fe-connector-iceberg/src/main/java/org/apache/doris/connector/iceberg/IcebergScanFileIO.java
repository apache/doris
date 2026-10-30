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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestListFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Request-local reads: cancellation never enters a catalog's shared FileIO or Doris manifest-cache loader. */
final class IcebergScanFileIO implements FileIO {
    private final FileIO delegate;
    private final Runnable checkCancelled;

    IcebergScanFileIO(FileIO delegate, Runnable checkCancelled) {
        this.delegate = delegate;
        this.checkCancelled = checkCancelled;
    }

    @Override
    public InputFile newInputFile(String path) {
        checkCancelled.run();
        return guarded(delegate.newInputFile(path));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        checkCancelled.run();
        return guarded(delegate.newInputFile(path, length));
    }

    @Override
    public InputFile newInputFile(DataFile file) {
        checkCancelled.run();
        return guarded(delegate.newInputFile(file));
    }

    @Override
    public InputFile newInputFile(DeleteFile file) {
        checkCancelled.run();
        return guarded(delegate.newInputFile(file));
    }

    @Override
    public InputFile newInputFile(ManifestFile file) {
        checkCancelled.run();
        return guarded(delegate.newInputFile(file));
    }

    @Override
    public InputFile newInputFile(ManifestListFile file) {
        checkCancelled.run();
        return guarded(delegate.newInputFile(file));
    }

    private InputFile guarded(InputFile input) {
        return new InputFile() {
            @Override
            public long getLength() {
                checkCancelled.run();
                return input.getLength();
            }

            @Override
            public boolean exists() {
                checkCancelled.run();
                return input.exists();
            }

            @Override
            public String location() {
                return input.location();
            }

            @Override
            public SeekableInputStream newStream() {
                checkCancelled.run();
                SeekableInputStream stream = input.newStream();
                return new SeekableInputStream() {
                    @Override
                    public long getPos() throws IOException {
                        return stream.getPos();
                    }

                    @Override
                    public void seek(long position) throws IOException {
                        checkCancelled.run();
                        stream.seek(position);
                    }

                    @Override
                    public int read() throws IOException {
                        checkCancelled.run();
                        return stream.read();
                    }

                    @Override
                    public int read(byte[] bytes, int offset, int length) throws IOException {
                        checkCancelled.run();
                        return stream.read(bytes, offset, length);
                    }

                    @Override
                    public long skip(long count) throws IOException {
                        checkCancelled.run();
                        return stream.skip(count);
                    }

                    @Override
                    public void close() throws IOException {
                        // Closing belongs to the SDK reader, including when cancelled.
                        stream.close();
                    }
                };
            }
        };
    }

    @Override
    public Map<String, String> properties() {
        checkCancelled.run();
        Map<String, String> properties = new HashMap<>(delegate.properties());
        // SDK ByteBuffer cache hits bypass our stream's checks. Keep the guard even for in-memory reads.
        // This view has request identity anyway; the catalog FileIO and Doris shared manifest cache are intact.
        properties.put(CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "false");
        return properties;
    }

    @Override
    public OutputFile newOutputFile(String path) {
        throw new UnsupportedOperationException("Split planning FileIO is read-only");
    }

    @Override
    public void deleteFile(String path) {
        throw new UnsupportedOperationException("Split planning FileIO is read-only");
    }

    @Override
    public void close() {
        // Borrowed from the catalog: a single source never closes the catalog-wide FileIO.
    }
}
