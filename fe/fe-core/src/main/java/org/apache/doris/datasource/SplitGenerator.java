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

package org.apache.doris.datasource;

import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.UserException;
import org.apache.doris.spi.Split;

import java.util.List;

/**
 * The Producer(e.g. ScanNode) that provides the file splits in lazy and batch mode.
 * The consumer should call `getNextBatch` to fetch the next batch of splits.
 */
public interface SplitGenerator {
    /**
     * Get all file splits if the producer doesn't support batch mode.
     */
    default List<Split> getSplits() throws UserException {
        // todo: remove this interface if batch mode is stable
        throw new NotImplementedException("Not implement");
    }

    /**
     * Whether the producer(e.g. ScanNode) support batch mode.
     */
    default boolean isBatchMode() {
        return false;
    }

    /**
     * Because file splits are generated lazily, the exact number of splits may not be known,
     * provide an estimated value to show in describe statement.
     */
    default int numApproximateSplits() {
        return -1;
    }

    default void startSplit() {
    }

    /**
     * Close split generator, and stop the split executor
     */
    default void stop() {
    }
}
