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

package org.apache.doris.filesystem.broker;

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;
import org.apache.doris.thrift.TBrokerCheckPathExistRequest;
import org.apache.doris.thrift.TBrokerCheckPathExistResponse;
import org.apache.doris.thrift.TBrokerDeletePathRequest;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerListPathRequest;
import org.apache.doris.thrift.TBrokerListResponse;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerRenamePathRequest;
import org.apache.doris.thrift.TBrokerVersion;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.thrift.TException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * {@link FileSystem} implementation backed by a Doris Broker process via Thrift RPC.
 *
 * <p>The broker endpoint (host, port) is pre-resolved by fe-core before construction.
 * This class has zero dependency on fe-core, {@code BrokerMgr}, or {@code BrokerProperties}.
 *
 * <p>Instances are created by {@link BrokerFileSystemProvider} via the SPI mechanism
 * after fe-core calls {@code FileSystemFactory.getBrokerFileSystem(host, port, params)}.
 */
public class BrokerSpiFileSystem implements FileSystem {

    private final TNetworkAddress endpoint;
    /** FE identifier sent to broker for logging (e.g. "host:editLogPort"). */
    private final String clientId;
    /** Broker-specific configuration params (username, password, hadoop conf, etc.). */
    private final Map<String, String> brokerParams;
    private final BrokerClientPool clientPool;

    BrokerSpiFileSystem(String host, int port, String clientId, Map<String, String> brokerParams) {
        this(new TNetworkAddress(host, port), clientId, brokerParams, new BrokerClientPool());
    }

    /** Package-visible constructor for unit testing with a mock {@link BrokerClientPool}. */
    BrokerSpiFileSystem(TNetworkAddress endpoint, String clientId,
            Map<String, String> brokerParams, BrokerClientPool clientPool) {
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.brokerParams = Map.copyOf(brokerParams);
        this.clientPool = clientPool;
    }

    @Override
    public boolean exists(Location location) throws IOException {
        TPaloBrokerService.Client client = clientPool.borrow(endpoint);
        boolean returnToPool = true;
        try {
            TBrokerCheckPathExistRequest req = new TBrokerCheckPathExistRequest(
                    TBrokerVersion.VERSION_ONE, location.uri(), brokerParams);
            TBrokerCheckPathExistResponse rep = client.checkPathExist(req);
            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() == TBrokerOperationStatusCode.FILE_NOT_FOUND) {
                return false;
            }
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new IOException("Failed to check path existence [" + location + "]: " + opst.getMessage());
            }
            return rep.isIsPathExist();
        } catch (TException e) {
            returnToPool = false;
            throw new IOException("Broker RPC failed for exists [" + location + "]: " + e.getMessage(), e);
        } finally {
            if (returnToPool) {
                clientPool.returnGood(endpoint, client);
            } else {
                clientPool.invalidate(endpoint, client);
            }
        }
    }

    /**
     * Empty-directory creation is not supported by the broker filesystem. The broker
     * Thrift IDL exposes no {@code mkdir}/{@code mkdirs} RPC, and the typical object-store
     * backends (e.g. S3) do not have first-class directories. Parent directories are created
     * implicitly by the broker on {@code openWriter}, so callers that produce files do not
     * need to call this method; callers that depend on the existence of an empty directory
     * (e.g. as a synchronization barrier) cannot be supported and must surface the error.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void mkdirs(Location location) throws IOException {
        throw new UnsupportedOperationException(
                "Broker filesystem does not support empty directory creation: " + location);
    }

    @Override
    public void delete(Location location, boolean recursive) throws IOException {
        if (!recursive) {
            // Broker's deletePath is unconditionally recursive server-side; emulate POSIX rmdir
            // by probing the path first and refusing to descend into non-empty directories.
            // listPath of a directory returns its child entries; of a file returns the file itself
            // (path equals location.uri()); of a missing path returns an empty list.
            List<TBrokerFileStatus> children = listPath(location.uri(), false);
            for (TBrokerFileStatus child : children) {
                if (!location.uri().equals(child.getPath())) {
                    throw new IOException("Directory not empty: " + location);
                }
            }
        }
        TPaloBrokerService.Client client = clientPool.borrow(endpoint);
        boolean returnToPool = true;
        try {
            TBrokerDeletePathRequest req = new TBrokerDeletePathRequest(
                    TBrokerVersion.VERSION_ONE, location.uri(), brokerParams);
            TBrokerOperationStatus opst = client.deletePath(req);
            if (opst.getStatusCode() == TBrokerOperationStatusCode.FILE_NOT_FOUND) {
                return;
            }
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new IOException("Failed to delete [" + location + "]: " + opst.getMessage());
            }
        } catch (TException e) {
            returnToPool = false;
            throw new IOException("Broker RPC failed for delete [" + location + "]: " + e.getMessage(), e);
        } finally {
            if (returnToPool) {
                clientPool.returnGood(endpoint, client);
            } else {
                clientPool.invalidate(endpoint, client);
            }
        }
    }

    @Override
    public void rename(Location src, Location dst) throws IOException {
        TPaloBrokerService.Client client = clientPool.borrow(endpoint);
        boolean returnToPool = true;
        try {
            TBrokerRenamePathRequest req = new TBrokerRenamePathRequest(
                    TBrokerVersion.VERSION_ONE, src.uri(), dst.uri(), brokerParams);
            TBrokerOperationStatus opst = client.renamePath(req);
            if (opst.getStatusCode() == TBrokerOperationStatusCode.FILE_NOT_FOUND) {
                throw new FileNotFoundException("Source path does not exist: " + src);
            }
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new IOException("Failed to rename [" + src + "] -> [" + dst + "]: " + opst.getMessage());
            }
        } catch (TException e) {
            returnToPool = false;
            throw new IOException("Broker RPC failed for rename: " + e.getMessage(), e);
        } finally {
            if (returnToPool) {
                clientPool.returnGood(endpoint, client);
            } else {
                clientPool.invalidate(endpoint, client);
            }
        }
    }

    /**
     * Atomic broker-side rename overload. Avoids the default {@code exists} + {@code rename}
     * sequence (TOCTOU race window) by interpreting a {@code FILE_NOT_FOUND} from the broker
     * as the missing-source signal and routing it to {@code whenSrcNotExists}.
     */
    @Override
    public void renameDirectory(Location src, Location dst, Runnable whenSrcNotExists)
            throws IOException {
        try {
            rename(src, dst);
        } catch (FileNotFoundException e) {
            whenSrcNotExists.run();
        }
    }

    @Override
    public FileIterator list(Location location) throws IOException {
        List<TBrokerFileStatus> statuses = listPath(location.uri(), false);
        List<FileEntry> entries = new ArrayList<>(statuses.size());
        for (TBrokerFileStatus s : statuses) {
            entries.add(new FileEntry(
                    Location.of(s.getPath()),
                    s.getSize(),
                    s.isIsDir(),
                    s.getModificationTime(),
                    null));
        }
        Iterator<FileEntry> it = entries.iterator();
        return new FileIterator() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public FileEntry next() {
                return it.next();
            }

            @Override
            public void close() {
                // no-op: list result is already fully materialized
            }
        };
    }

    /**
     * Override that issues a single recursive {@code listPath} RPC instead of the default
     * implementation's depth-first traversal (one RPC per directory). The broker's
     * {@code TBrokerListPathRequest.recursive=true} flag returns every descendant in one
     * round trip, so this avoids O(depth) latency on deep trees.
     */
    @Override
    public List<FileEntry> listFilesRecursive(Location dir) throws IOException {
        List<TBrokerFileStatus> statuses = listPath(dir.uri(), true);
        List<FileEntry> result = new ArrayList<>(statuses.size());
        for (TBrokerFileStatus s : statuses) {
            if (s.isIsDir()) {
                continue;
            }
            result.add(new FileEntry(
                    Location.of(s.getPath()),
                    s.getSize(),
                    false,
                    s.getModificationTime(),
                    null));
        }
        return result;
    }

    /**
     * Glob listing backed by the broker's native glob support: {@code listPath} delegates to
     * Hadoop {@code FileSystem.globStatus} on the broker side, so a single non-recursive RPC
     * returns all matching entries. Pagination cursor semantics mirror the S3/Azure
     * implementations: when a page limit is hit and another match exists past it, that match
     * becomes {@link GlobListing#getMaxFile()}; otherwise it is the last matched key (or
     * empty when no entries matched).
     */
    @Override
    public GlobListing globListWithLimit(Location path, String startAfter, long maxBytes,
            long maxFiles) throws IOException {
        List<TBrokerFileStatus> statuses = listPath(path.uri(), false);
        List<FileEntry> files = new ArrayList<>();
        long totalSize = 0L;
        boolean reachLimit = false;
        String nextMatchAfterLimit = "";
        String lastMatchedKey = "";
        for (TBrokerFileStatus s : statuses) {
            if (s.isIsDir()) {
                continue;
            }
            String key = s.getPath();
            if (startAfter != null && !startAfter.isEmpty() && key.compareTo(startAfter) <= 0) {
                continue;
            }
            if (reachLimit) {
                if (nextMatchAfterLimit.isEmpty()) {
                    nextMatchAfterLimit = key;
                }
                continue;
            }
            files.add(new FileEntry(
                    Location.of(key),
                    s.getSize(),
                    false,
                    s.getModificationTime(),
                    null));
            totalSize += s.getSize();
            lastMatchedKey = key;
            if ((maxFiles > 0 && files.size() >= maxFiles)
                    || (maxBytes > 0 && totalSize >= maxBytes)) {
                reachLimit = true;
            }
        }
        String maxFile = reachLimit && !nextMatchAfterLimit.isEmpty()
                ? nextMatchAfterLimit
                : lastMatchedKey;
        // Broker has no bucket concept; surface the original glob URI as the prefix for diagnostics.
        return new GlobListing(files, "", path.uri(), maxFile);
    }

    @Override
    public DorisInputFile newInputFile(Location location) throws IOException {
        return new BrokerInputFile(this, location, -1L, endpoint, clientId, brokerParams, clientPool);
    }

    @Override
    public DorisInputFile newInputFile(Location location, long length) throws IOException {
        return new BrokerInputFile(this, location, length, endpoint, clientId, brokerParams, clientPool);
    }

    @Override
    public DorisOutputFile newOutputFile(Location location) throws IOException {
        return new BrokerOutputFile(this, location, endpoint, clientId, brokerParams, clientPool);
    }

    @Override
    public void close() throws IOException {
        clientPool.close();
    }

    /**
     * Calls broker {@code listPath} RPC and returns the raw file status list.
     * The broker handles glob patterns natively (delegates to Hadoop FileSystem.globStatus).
     */
    List<TBrokerFileStatus> listPath(String path, boolean recursive) throws IOException {
        TPaloBrokerService.Client client = clientPool.borrow(endpoint);
        boolean returnToPool = true;
        try {
            TBrokerListPathRequest req = new TBrokerListPathRequest(
                    TBrokerVersion.VERSION_ONE, path, recursive, brokerParams);
            TBrokerListResponse rep = client.listPath(req);
            TBrokerOperationStatus opst = rep.getOpStatus();
            if (opst.getStatusCode() == TBrokerOperationStatusCode.FILE_NOT_FOUND) {
                return List.of();
            }
            if (opst.getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new IOException("Failed to list path [" + path + "]: " + opst.getMessage());
            }
            return rep.getFiles() != null ? rep.getFiles() : List.of();
        } catch (TException e) {
            returnToPool = false;
            throw new IOException("Broker RPC failed for list [" + path + "]: " + e.getMessage(), e);
        } finally {
            if (returnToPool) {
                clientPool.returnGood(endpoint, client);
            } else {
                clientPool.invalidate(endpoint, client);
            }
        }
    }

    TNetworkAddress endpoint() {
        return endpoint;
    }

    String clientId() {
        return clientId;
    }

    Map<String, String> brokerParams() {
        return brokerParams;
    }
}
