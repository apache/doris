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

package org.apache.doris.fs.remote;

import org.apache.doris.common.CustomThreadFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The RemoteFSPhantomManager class is responsible for managing the phantom references
 * of RemoteFileSystem objects. It ensures that the associated FileSystem resources are
 * automatically cleaned up when the RemoteFileSystem objects are garbage collected.
 * <p>
 * By utilizing a ReferenceQueue and PhantomReference, this class can monitor the lifecycle
 * of RemoteFileSystem objects. When a RemoteFileSystem object is no longer in use and is
 * garbage collected, its corresponding FileSystem resource is properly closed to prevent
 * resource leaks.
 * <p>
 * The class provides a thread-safe mechanism to ensure that the cleanup thread is started only once.
 * <p>
 * Main functionalities include:
 * - Registering phantom references of RemoteFileSystem objects.
 * - Starting a periodic cleanup thread that automatically closes unused FileSystem resources.
 */
public class RemoteFSPhantomManager {

    private static final Logger LOG = LogManager.getLogger(RemoteFSPhantomManager.class);

    // Scheduled executor for periodic resource cleanup
    private static ScheduledExecutorService cleanupExecutor;

    // Reference queue for monitoring RemoteFileSystem objects' phantom references
    private static final ReferenceQueue<RemoteFileSystem> referenceQueue = new ReferenceQueue<>();

    // Map storing the phantom references and their corresponding FileSystem objects
    private static final ConcurrentHashMap<PhantomReference<RemoteFileSystem>, FileSystem> referenceMap =
            new ConcurrentHashMap<>();
    /**
     * Map for tracking reference counts of FileSystem instances.
     * Key: FileSystem instance, Value: AtomicInteger representing the reference count for the FileSystem.
     * This ensures that the FileSystem is only closed when all associated RemoteFileSystem instances are
     * garbage collected.
     */
    private static final ConcurrentHashMap<FileSystem, AtomicInteger> fileSystemReferenceCounts =
            new ConcurrentHashMap<>();

    // Flag indicating whether the cleanup thread has been started
    private static final AtomicBoolean isStarted = new AtomicBoolean(false);

    /**
     * Registers a phantom reference for a RemoteFileSystem object in the manager.
     * If the cleanup thread has not been started, it will be started.
     *
     * @param remoteFileSystem the RemoteFileSystem object to be registered
     */
    public static synchronized void registerPhantomReference(RemoteFileSystem remoteFileSystem) {
        if (!isStarted.get()) {
            start();
            isStarted.set(true);
        }
        FileSystem fileSystem = remoteFileSystem.dfsFileSystem;
        fileSystemReferenceCounts.compute(fileSystem, (fs, count) -> {
            if (count == null) {
                LOG.info("New FileSystem detected: {}", fileSystem.getUri());
                return new AtomicInteger(1);
            } else {
                LOG.info("Incrementing reference count for FileSystem: {}", fileSystem.getUri());
                count.incrementAndGet();
                return count;
            }
        });

        RemoteFileSystemPhantomReference phantomReference = new RemoteFileSystemPhantomReference(remoteFileSystem,
                referenceQueue);
        referenceMap.put(phantomReference, fileSystem);
    }

    /**
     * Starts the cleanup thread, which periodically checks and cleans up unused FileSystem resources.
     * The method uses double-checked locking to ensure thread-safe startup of the cleanup thread.
     */
    public static void start() {
        if (isStarted.compareAndSet(false, true)) {
            synchronized (RemoteFSPhantomManager.class) {
                LOG.info("Starting cleanup thread for RemoteFileSystem objects");
                if (cleanupExecutor == null) {
                    CustomThreadFactory threadFactory = new CustomThreadFactory("remote-fs-phantom-cleanup");
                    cleanupExecutor = Executors.newScheduledThreadPool(1, threadFactory);
                    cleanupExecutor.scheduleAtFixedRate(() -> {
                        Reference<? extends RemoteFileSystem> ref;
                        while ((ref = referenceQueue.poll()) != null) {
                            RemoteFileSystemPhantomReference phantomRef = (RemoteFileSystemPhantomReference) ref;

                            FileSystem fs = referenceMap.remove(phantomRef);
                            if (fs != null) {
                                fileSystemReferenceCounts.computeIfPresent(fs, (key, count) -> {
                                    int remaining = count.decrementAndGet();
                                    if (remaining <= 0) {
                                        try {
                                            fs.close();
                                            LOG.info("Closed FileSystem: {}", fs.getUri());
                                            return null; // Remove the FileSystem from the map
                                        } catch (IOException e) {
                                            LOG.warn("Failed to close FileSystem: {}", fs.getUri(), e);
                                        }
                                    } else {
                                        LOG.info("Decrementing reference count for FileSystem: {}, "
                                                + "remaining: {}", fs.getUri(), remaining);
                                    }
                                    return count;
                                });
                            }
                        }
                    }, 0, 1, TimeUnit.MINUTES);
                }
            }
        }
    }

}
