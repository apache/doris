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

package org.apache.doris.fs.remote.dfs;

import org.apache.doris.common.CustomThreadFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The RemoteFSPhantomManager class is responsible for managing the phantom references
 * of DFSFileSystem objects. It ensures that the associated FileSystem resources are
 * automatically cleaned up when the DFSFileSystem objects are garbage collected.
 * <p>
 * By utilizing a ReferenceQueue and PhantomReference, this class can monitor the lifecycle
 * of DFSFileSystem objects. When a DFSFileSystem object is no longer in use and is
 * garbage collected, its corresponding FileSystem resource is properly closed to prevent
 * resource leaks.
 * <p>
 * The class provides a thread-safe mechanism to ensure that the cleanup thread is started only once.
 * <p>
 * Main functionalities include:
 * - Registering phantom references of DFSFileSystem objects.
 * - Starting a periodic cleanup thread that automatically closes unused FileSystem resources.
 */
public class RemoteFSPhantomManager {

    private static final Logger LOG = LogManager.getLogger(RemoteFSPhantomManager.class);

    // Scheduled executor for periodic resource cleanup
    private static ScheduledExecutorService cleanupExecutor;

    // Reference queue for monitoring DFSFileSystem objects' phantom references
    private static final ReferenceQueue<DFSFileSystem> referenceQueue = new ReferenceQueue<>();

    // Map storing the phantom references and their corresponding FileSystem resources
    private static final ConcurrentHashMap<Reference<? extends DFSFileSystem>, DFSFileSystemResource> referenceMap
            = new ConcurrentHashMap<>();

    // Flag indicating whether the cleanup thread has been started
    private static final AtomicBoolean isStarted = new AtomicBoolean(false);

    /**
     * Registers a phantom reference for a DFSFileSystem object in the manager.
     * If the cleanup thread has not been started, it will be started.
     *
     * @param remoteFileSystem the DFSFileSystem object to be registered
     */
    static void registerPhantomReference(DFSFileSystem remoteFileSystem, DFSFileSystemResource resource) {
        start();
        PhantomReference<DFSFileSystem> phantomReference = new PhantomReference<>(remoteFileSystem, referenceQueue);
        referenceMap.put(phantomReference, resource);
    }

    /**
     * Starts the cleanup thread, which periodically checks and cleans up unused FileSystem resources.
     * The method uses an atomic flag to ensure thread-safe startup of the cleanup thread.
     */
    public static void start() {
        if (isStarted.compareAndSet(false, true)) {
            LOG.info("Starting cleanup thread for DFSFileSystem objects");
            CustomThreadFactory threadFactory = new CustomThreadFactory("remote-fs-phantom-cleanup");
            cleanupExecutor = Executors.newScheduledThreadPool(1, threadFactory);
            cleanupExecutor.scheduleAtFixedRate(() -> {
                Reference<? extends DFSFileSystem> ref;
                while ((ref = referenceQueue.poll()) != null) {
                    DFSFileSystemResource resource = referenceMap.remove(ref);
                    try {
                        resource.requestClose();
                    } catch (RuntimeException e) {
                        LOG.warn("Failed to close file system resource", e);
                    }
                }
            }, 0, 1, TimeUnit.MINUTES);
        }
    }

}
