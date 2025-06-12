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

import org.apache.doris.fs.FileSystem;
import org.apache.doris.fs.io.DorisOutputFile;
import org.apache.doris.fs.io.DorisPath;

import com.google.common.io.CountingOutputStream;
import static java.util.Objects.requireNonNull;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;

public class DelegateOutputFile implements OutputFile {
    private final FileSystem fileSystem;
    private final DorisOutputFile outputFile;

    public DelegateOutputFile(FileSystem fileSystem, DorisPath path) {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.outputFile = fileSystem.newOutputFile(path);
    }

    @Override
    public PositionOutputStream create() {
        try {
            return new CountingPositionOutputStream(outputFile.create());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create file: " + location(), e);
        }
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
        try {
            return new CountingPositionOutputStream(outputFile.createOrOverwrite());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create file: " + location(), e);
        }
    }

    @Override
    public String location() {
        return outputFile.path().toString();
    }

    @Override
    public InputFile toInputFile() {
        return new DelegateInputFile(fileSystem.newInputFile(outputFile.path()));
    }

    @Override
    public String toString() {
        return outputFile.toString();
    }

    private static class CountingPositionOutputStream extends PositionOutputStream {
        private final CountingOutputStream stream;

        private CountingPositionOutputStream(OutputStream stream) {
            this.stream = new CountingOutputStream(stream);
        }

        @Override
        public long getPos() {
            return stream.getCount();
        }

        @Override
        public void write(int b) throws IOException {
            stream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            stream.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            stream.flush();
        }

        @Override
        public void close() throws IOException {
            stream.close();
        }
    }
}
