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

package org.apache.orc;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * A memory manager that keeps a global context of how many ORC
 * writers there are and manages the memory between them. For use cases with
 * dynamic partitions, it is easy to end up with many writers in the same task.
 * By managing the size of each allocation, we try to cut down the size of each
 * allocation and keep the task from running out of memory.
 * <p>
 * This class is not thread safe, but is re-entrant - ensure creation and all
 * invocations are triggered from the same thread.
 */
public interface MemoryManager {

  interface Callback {
    /**
     * The scale factor for the stripe size has changed and thus the
     * writer should adjust their desired size appropriately.
     * @param newScale the current scale factor for memory allocations
     * @return true if the writer was over the limit
     */
    boolean checkMemory(double newScale) throws IOException;
  }

  /**
   * Add a new writer's memory allocation to the pool. We use the path
   * as a unique key to ensure that we don't get duplicates.
   * @param path the file that is being written
   * @param requestedAllocation the requested buffer size
   */
  void addWriter(Path path, long requestedAllocation,
                 Callback callback) throws IOException;

  /**
   * Remove the given writer from the pool.
   * @param path the file that has been closed
   */
  void removeWriter(Path path) throws IOException;

  /**
   * Give the memory manager an opportunity for doing a memory check.
   * @param rows number of rows added
   * @throws IOException
   * @deprecated Use {@link MemoryManager#checkMemory} instead
   */
  void addedRow(int rows) throws IOException;

  /**
   * As part of adding rows, the writer calls this method to determine
   * if the scale factor has changed. If it has changed, the Callback will be
   * called.
   * @param previousAllocation the previous allocation
   * @param writer the callback to call back into if we need to
   * @return the current allocation
   */
  default long checkMemory(long previousAllocation,
                           Callback writer) throws IOException {
    addedRow(1024);
    return previousAllocation;
  }
}
