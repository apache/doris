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

import org.apache.doris.kerberos.HadoopAuthenticator;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link FileIO} decorator that runs every file-<b>factory</b> call ({@code newInputFile} / {@code newOutputFile}
 * / {@code deleteFile}) inside a plugin-side Kerberos {@code doAs}. Installed by
 * {@code IcebergConnectorTransaction.openTransaction} for a Kerberos catalog so that iceberg's parallel manifest
 * writes — fanned onto the shared {@code ThreadPools.getWorkerPool()}, which runs OUTSIDE the caller-thread
 * {@code doAs} — still authenticate against secured HDFS.
 *
 * <p><b>Why wrapping only the factory methods suffices.</b> {@code HadoopFileIO.newOutputFile}/{@code newInputFile}
 * resolve and capture the {@code FileSystem} (via {@code FileSystem.get(uri, conf)}, keyed by
 * {@code UserGroupInformation.getCurrentUser()}) at factory-call time and hand it to the returned
 * {@code HadoopOutputFile}/{@code HadoopInputFile}. Running the factory call under {@code doAs} therefore captures
 * the <i>Kerberos</i> FileSystem (whose cached {@code DFSClient} proxy is bound to the Kerberos UGI); the deferred
 * stream I/O ({@code createOrOverwrite()} / {@code newStream()}), even when it later runs on an unauthenticated
 * worker-pool thread, reuses that FileSystem's Kerberos connection. {@code deleteFile} resolves the FileSystem and
 * issues the delete in one call, so wrapping it covers both. The streams need no wrapping.
 *
 * <p>The {@code doAs} is an exact mirror of {@code HadoopExecutionAuthenticator.execute}
 * ({@code hadoopAuthenticator.doAs(action)}) — the same single-owner authenticator instance
 * {@link TcclPinningConnectorContext} uses on the caller thread. {@code IOException} from the authenticator is
 * surfaced as {@link UncheckedIOException} because the {@link FileIO} factory methods declare no checked exception
 * (iceberg wraps it into its own {@code RuntimeIOException} at the call site, as it does for a raw factory failure).
 *
 * <p>The bundled {@code HadoopFileIO} additionally implements {@code DelegateFileIO} (bulk / prefix ops), used only
 * by maintenance actions (orphan-file cleanup), never by the append/rewrite commit path exercised here; a caller
 * that probes for those interfaces simply falls back to per-file operations, which route through the wrapped
 * primitives above. Kept a plain {@link FileIO} deliberately to avoid coupling to that optional surface.
 */
final class IcebergAuthenticatedFileIO implements FileIO {

    private final FileIO delegate;
    private final HadoopAuthenticator authenticator;

    IcebergAuthenticatedFileIO(FileIO delegate, HadoopAuthenticator authenticator) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.authenticator = Objects.requireNonNull(authenticator, "authenticator");
    }

    @Override
    public InputFile newInputFile(String path) {
        return doAs(() -> delegate.newInputFile(path));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        return doAs(() -> delegate.newInputFile(path, length));
    }

    @Override
    public OutputFile newOutputFile(String path) {
        return doAs(() -> delegate.newOutputFile(path));
    }

    @Override
    public void deleteFile(String path) {
        doAs(() -> {
            delegate.deleteFile(path);
            return null;
        });
    }

    @Override
    public Map<String, String> properties() {
        return delegate.properties();
    }

    @Override
    public void initialize(Map<String, String> properties) {
        delegate.initialize(properties);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private <T> T doAs(PrivilegedExceptionAction<T> action) {
        try {
            return authenticator.doAs(action);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
