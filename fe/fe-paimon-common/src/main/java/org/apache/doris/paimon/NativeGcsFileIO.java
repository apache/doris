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

package org.apache.doris.paimon;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.hadoop.HadoopFileIO;

import java.io.IOException;

/** Routes legacy GCS aliases through the native Hadoop GCS connector, including persisted file paths. */
public class NativeGcsFileIO extends HadoopFileIO {
    private static final long serialVersionUID = 1L;

    public NativeGcsFileIO(Path path) {
        super(normalize(path));
    }

    public static Path normalize(Path path) {
        String scheme = path.toUri().getScheme();
        if ("s3".equalsIgnoreCase(scheme) || "s3a".equalsIgnoreCase(scheme)) {
            return new Path("gs" + path.toString().substring(scheme.length()));
        }
        return path;
    }

    @Override
    public boolean isObjectStore() {
        return true;
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        return super.newInputStream(normalize(path));
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return super.newOutputStream(normalize(path), overwrite);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return super.getFileStatus(normalize(path));
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return super.listStatus(normalize(path));
    }

    @Override
    public RemoteIterator<FileStatus> listFilesIterative(Path path, boolean recursive) throws IOException {
        return super.listFilesIterative(normalize(path), recursive);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return super.exists(normalize(path));
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return super.delete(normalize(path), recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return super.mkdirs(normalize(path));
    }

    @Override
    public boolean rename(Path source, Path target) throws IOException {
        return super.rename(normalize(source), normalize(target));
    }

    @Override
    public void overwriteFileUtf8(Path path, String content) throws IOException {
        super.overwriteFileUtf8(normalize(path), content);
    }

    @Override
    public boolean tryAtomicOverwriteViaRename(Path path, String content) throws IOException {
        return super.tryAtomicOverwriteViaRename(normalize(path), content);
    }

    public static final class Loader implements FileIOLoader {
        private static final long serialVersionUID = 1L;

        @Override
        public String getScheme() {
            return "gs";
        }

        @Override
        public FileIO load(Path path) {
            return "gs".equalsIgnoreCase(normalize(path).toUri().getScheme())
                    ? new NativeGcsFileIO(path) : new HadoopFileIO(path);
        }
    }
}
