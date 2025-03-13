/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util.collection;

import org.apache.hudi.common.util.FileIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

/* Copied From
 * https://github.com/apache/hudi/blob/release-0.15.0/hudi-common/src/main/java/org/apache/hudi/common/util/collection/DiskMap.java
 * Doris Modification.
 * Use Static cleaner class to avoid circular references in shutdown hooks
 */

/**
 * This interface provides the map interface for storing records in disk after
 * they
 * spill over from memory. Used by {@link ExternalSpillableMap}.
 *
 * @param <T> The generic type of the keys
 * @param <R> The generic type of the values
 */
public abstract class DiskMap<T extends Serializable, R extends Serializable> implements Map<T, R>, Iterable<R> {

  private static final Logger LOG = LoggerFactory.getLogger(DiskMap.class);
  private static final String SUBFOLDER_PREFIX = "hudi";
  private final File diskMapPathFile;
  private transient Thread shutdownThread = null;

  // Base path for the write file
  protected final String diskMapPath;

  public DiskMap(String basePath, String prefix) throws IOException {
    this.diskMapPath = String.format("%s/%s-%s-%s", basePath, SUBFOLDER_PREFIX, prefix, UUID.randomUUID().toString());
    diskMapPathFile = new File(diskMapPath);
    FileIOUtils.deleteDirectory(diskMapPathFile);
    FileIOUtils.mkdir(diskMapPathFile);
    // Make sure the folder is deleted when JVM exits
    diskMapPathFile.deleteOnExit();
    addShutDownHook();
  }

  /**
   * Register shutdown hook to force flush contents of the data written to
   * FileOutputStream from OS page cache
   * (typically 4 KB) to disk.
   */
  private void addShutDownHook() {
    // Register this disk map path with the static cleaner instead of using an
    // instance-specific hook
    DiskMapCleaner.registerForCleanup(diskMapPath);
  }

  /**
   * @returns a stream of the values stored in the disk.
   */
  abstract Stream<R> valueStream();

  /**
   * Number of bytes spilled to disk.
   */
  abstract long sizeOfFileOnDiskInBytes();

  /**
   * Close and cleanup the Map.
   */
  public void close() {
    cleanup(false);
  }

  /**
   * Cleanup all resources, files and folders
   * triggered by shutdownhook.
   */
  private void cleanup() {
    cleanup(true);
  }

  /**
   * Cleanup all resources, files and folders.
   */
  private void cleanup(boolean isTriggeredFromShutdownHook) {
    // Reuse the static cleaner method to clean the directory
    DiskMapCleaner.cleanupDirectory(diskMapPath);

    // Deregister from the static cleaner
    if (!isTriggeredFromShutdownHook) {
      DiskMapCleaner.deregisterFromCleanup(diskMapPath);
    }
  }

  /**
   * Static cleaner class to avoid circular references in shutdown hooks
   */
  private static class DiskMapCleaner {
    private static final Logger CLEANER_LOG = LoggerFactory.getLogger(DiskMapCleaner.class);
    private static final Set<String> PATHS_TO_CLEAN = Collections.synchronizedSet(new HashSet<>());
    private static final Thread SHUTDOWN_HOOK;

    static {
      // Register a single JVM-wide shutdown hook that handles all paths
      SHUTDOWN_HOOK = new Thread(() -> {
        synchronized (PATHS_TO_CLEAN) {
          PATHS_TO_CLEAN.forEach(DiskMapCleaner::cleanupDirectory);
          PATHS_TO_CLEAN.clear();
        }
      });
      Runtime.getRuntime().addShutdownHook(SHUTDOWN_HOOK);
    }

    /**
     * Register a path to be cleaned up when JVM exits
     * 
     * @param directoryPath Path to register for cleanup
     */
    public static void registerForCleanup(String directoryPath) {
      PATHS_TO_CLEAN.add(directoryPath);
    }

    /**
     * Deregister a path from cleanup when it's manually cleaned
     * 
     * @param directoryPath Path to deregister from cleanup
     */
    public static void deregisterFromCleanup(String directoryPath) {
      PATHS_TO_CLEAN.remove(directoryPath);
    }

    /**
     * Static cleanup method that doesn't hold references to DiskMap instances
     * 
     * @param directoryPath Path to the directory that needs to be cleaned up
     */
    public static void cleanupDirectory(String directoryPath) {
      try {
        FileIOUtils.deleteDirectory(new File(directoryPath));
      } catch (IOException exception) {
        CLEANER_LOG.warn("Error while deleting the disk map directory=" + directoryPath, exception);
      }
    }
  }
}
