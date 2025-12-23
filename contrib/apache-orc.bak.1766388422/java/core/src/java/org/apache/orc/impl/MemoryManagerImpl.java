/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.MemoryManager;
import org.apache.orc.OrcConf;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements a memory manager that keeps a global context of how many ORC
 * writers there are and manages the memory between them. For use cases with
 * dynamic partitions, it is easy to end up with many writers in the same task.
 * By managing the size of each allocation, we try to cut down the size of each
 * allocation and keep the task from running out of memory.
 * <p>
 * This class is not thread safe, but is re-entrant - ensure creation and all
 * invocations are triggered from the same thread.
 */
public class MemoryManagerImpl implements MemoryManager {

  private final long totalMemoryPool;
  private final Map<Path, WriterInfo> writerList = new HashMap<>();
  private final AtomicLong totalAllocation = new AtomicLong(0);

  private static class WriterInfo {
    long allocation;
    WriterInfo(long allocation) {
      this.allocation = allocation;
    }
  }

  /**
   * Create the memory manager.
   * @param conf use the configuration to find the maximum size of the memory
   *             pool.
   */
  public MemoryManagerImpl(Configuration conf) {
    this(Math.round(ManagementFactory.getMemoryMXBean().
        getHeapMemoryUsage().getMax() * OrcConf.MEMORY_POOL.getDouble(conf)));
  }

  /**
   * Create the memory manager
   * @param poolSize the size of memory to use
   */
  public MemoryManagerImpl(long poolSize) {
    totalMemoryPool = poolSize;
  }

  /**
   * Add a new writer's memory allocation to the pool. We use the path
   * as a unique key to ensure that we don't get duplicates.
   * @param path the file that is being written
   * @param requestedAllocation the requested buffer size
   */
  @Override
  public synchronized void addWriter(Path path, long requestedAllocation,
                              Callback callback) throws IOException {
    WriterInfo oldVal = writerList.get(path);
    // this should always be null, but we handle the case where the memory
    // manager wasn't told that a writer wasn't still in use and the task
    // starts writing to the same path.
    if (oldVal == null) {
      oldVal = new WriterInfo(requestedAllocation);
      writerList.put(path, oldVal);
      totalAllocation.addAndGet(requestedAllocation);
    } else {
      // handle a new writer that is writing to the same path
      totalAllocation.addAndGet(requestedAllocation - oldVal.allocation);
      oldVal.allocation = requestedAllocation;
    }
  }

  /**
   * Remove the given writer from the pool.
   * @param path the file that has been closed
   */
  @Override
  public synchronized void removeWriter(Path path) throws IOException {
    WriterInfo val = writerList.remove(path);
    if (val != null) {
      totalAllocation.addAndGet(-val.allocation);
    }
  }

  /**
   * Get the total pool size that is available for ORC writers.
   * @return the number of bytes in the pool
   */
  public long getTotalMemoryPool() {
    return totalMemoryPool;
  }

  /**
   * The scaling factor for each allocation to ensure that the pool isn't
   * oversubscribed.
   * @return a fraction between 0.0 and 1.0 of the requested size that is
   * available for each writer.
   */
  public double getAllocationScale() {
    long alloc = totalAllocation.get();
    return alloc <= totalMemoryPool ? 1.0 : (double) totalMemoryPool / alloc;
  }

  @Override
  public void addedRow(int rows) throws IOException {
    // PASS
  }

  /**
   * Obsolete method left for Hive, which extends this class.
   * @deprecated remove this method
   */
  public void notifyWriters() throws IOException {
    // PASS
  }

  @Override
  public long checkMemory(long previous, Callback writer) throws IOException {
    long current = totalAllocation.get();
    if (current != previous) {
      writer.checkMemory(getAllocationScale());
    }
    return current;
  }
}
