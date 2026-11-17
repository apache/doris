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

import org.apache.doris.connector.spi.ConnectorContext;

import com.google.common.util.concurrent.Uninterruptibles;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.OutputFile;
import io.delta.kernel.defaults.engine.hadoopio.HadoopInputFile;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * Read-only DefaultEngine IO with catalog-owned Hadoop clients.
 *
 * <p>Kernel's default HadoopFileIO obtains a filesystem for each operation but never closes it.
 * With fs.hdfs.impl.disable.cache=true that creates a new client on each read. Instead, retain one
 * private newInstance per authority and close exactly these clients, without closing another catalog's
 * global Hadoop cache entries. Directory discovery borrows the same instances.
 */
final class DeltaLakeFileIO implements FileIO, AutoCloseable {
    private final Configuration configuration;
    private final ConnectorContext context;
    // Publish the task under this monitor, then initialize outside it: different authorities and
    // catalog close must not wait for a network connection while holding this lock.
    private final Map<String, FutureTask<FileSystem>> fileSystems = new HashMap<>();
    private boolean closed;

    DeltaLakeFileIO(Configuration configuration, ConnectorContext context) {
        this.configuration = new Configuration(configuration);
        this.configuration.setClassLoader(DeltaLakeFileIO.class.getClassLoader());
        this.context = context;
    }

    FileSystem fileSystem(Path path) throws IOException {
        URI uri = path.makeQualified(FileSystem.getDefaultUri(configuration), new Path("/")).toUri();
        String key = uri.getScheme() + "://" + uri.getAuthority();
        FutureTask<FileSystem> task;
        synchronized (fileSystems) {
            if (closed) {
                throw new IOException("Delta catalog filesystem is closed");
            }
            task = fileSystems.computeIfAbsent(key, ignored -> new FutureTask<>(
                    () -> context.executeAuthenticated(() -> FileSystem.newInstance(uri, configuration))));
        }
        // FutureTask runs its callable once, including when several readers reach the same authority.
        task.run();
        try {
            return task.get();
        } catch (InterruptedException e) {
            // A waiting query may be cancelled while another query initializes the shared client.
            // Keep that initialization cached for its other readers; only this wait is cancelled.
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted opening Delta filesystem for " + path, e);
        } catch (ExecutionException e) {
            synchronized (fileSystems) {
                fileSystems.remove(key, task);
            }
            throw new IOException("Failed to open Delta filesystem for " + path, e.getCause());
        }
    }

    @Override
    public CloseableIterator<FileStatus> listFrom(String filePath) throws IOException {
        Path path = new Path(filePath);
        org.apache.hadoop.fs.FileStatus[] entries = fileSystem(path).listStatus(path.getParent());
        Arrays.sort(entries, Comparator.comparing(entry -> entry.getPath().getName()));
        Iterator<FileStatus> files = Arrays.stream(entries)
                .filter(entry -> entry.isFile() && entry.getPath().getName().compareTo(path.getName()) >= 0)
                .map(DeltaLakeFileIO::status)
                .iterator();
        return new CloseableIterator<FileStatus>() {
            @Override
            public boolean hasNext() {
                return files.hasNext();
            }

            @Override
            public FileStatus next() {
                return files.next();
            }

            @Override
            public void close() {
                // Hadoop listStatus has already released its listing resources.
            }
        };
    }

    @Override
    public FileStatus getFileStatus(String path) throws IOException {
        Path file = new Path(path);
        return status(fileSystem(file).getFileStatus(file));
    }

    private static FileStatus status(org.apache.hadoop.fs.FileStatus file) {
        return FileStatus.of(file.getPath().toString(), file.getLen(), file.getModificationTime());
    }

    @Override
    public String resolvePath(String path) throws IOException {
        Path file = new Path(path);
        return fileSystem(file).makeQualified(file).toString();
    }

    @Override
    public InputFile newInputFile(String path, long fileSize) {
        Path file = new Path(path);
        try {
            return new HadoopInputFile(fileSystem(file), file, fileSize);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open Delta input " + path, e);
        }
    }

    @Override
    public Optional<String> getConf(String key) {
        return Optional.ofNullable(configuration.get(key));
    }

    @Override
    public boolean mkdirs(String path) {
        throw new UnsupportedOperationException("Delta Lake connector is read-only");
    }

    @Override
    public OutputFile newOutputFile(String path) {
        throw new UnsupportedOperationException("Delta Lake connector is read-only");
    }

    @Override
    public boolean delete(String path) {
        throw new UnsupportedOperationException("Delta Lake connector is read-only");
    }

    @Override
    public void copyFileAtomically(String source, String destination, boolean overwrite) {
        throw new UnsupportedOperationException("Delta Lake connector is read-only");
    }

    @Override
    public void close() throws IOException {
        List<FutureTask<FileSystem>> owned;
        synchronized (fileSystems) {
            closed = true;
            owned = new ArrayList<>(fileSystems.values());
            fileSystems.clear();
        }
        IOException failure = null;
        for (FutureTask<FileSystem> task : owned) {
            try {
                Uninterruptibles.getUninterruptibly(task).close();
            } catch (ExecutionException e) {
                // Initialization failed and yielded no filesystem; the reading thread reports it.
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
