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

import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;

/**
 * When file splits are supplied in batch mode, splits are generated lazily and assigned in each call of `getNextBatch`.
 * `SplitGenerator` provides the file splits, and `FederationBackendPolicy` assigns these splits to backends.
 */
public class SplitAssignment {
    // magic number to estimate how many splits are allocated to BE in each batch
    private static final int NUM_SPLITS_PER_BE = 1024;
    // magic number to estimate how many splits are generated of each partition in each batch.
    private static final int NUM_SPLITS_PER_PARTITION = 10;

    private final FederationBackendPolicy backendPolicy;
    private final SplitGenerator splitGenerator;
    // Store the current assignment of file splits
    private final Multimap<Backend, Split> assignment;
    private final int maxBatchSize;

    public SplitAssignment(FederationBackendPolicy backendPolicy, SplitGenerator splitGenerator) {
        this.backendPolicy = backendPolicy;
        this.splitGenerator = splitGenerator;
        this.assignment = ArrayListMultimap.create();
        int numPartitions = ConnectContext.get().getSessionVariable().getNumPartitionsInBatchMode();
        maxBatchSize = Math.min(NUM_SPLITS_PER_PARTITION * numPartitions,
                NUM_SPLITS_PER_BE * backendPolicy.numBackends());
    }

    public void init() throws UserException {
        if (assignment.isEmpty() && splitGenerator.hasNext()) {
            assignment.putAll(backendPolicy.computeScanRangeAssignment(splitGenerator.getNextBatch(maxBatchSize)));
        }
    }

    public Multimap<Backend, Split> getCurrentAssignment() {
        return assignment;
    }

    public int numApproximateSplits() {
        return splitGenerator.numApproximateSplits();
    }

    public synchronized Collection<Split> getNextBatch(Backend backend) throws UserException {
        // Each call should consume all splits
        Collection<Split> splits = assignment.removeAll(backend);
        while (splits.isEmpty()) {
            // Get the next batch of splits, and assign to backends
            // If there is data skewing, it maybe causes splits to accumulate on some BE
            if (!splitGenerator.hasNext()) {
                return splits;
            }
            // todo: In each batch, it's to find the optimal assignment for partial splits,
            //  how to solve the global data skew?
            assignment.putAll(backendPolicy.computeScanRangeAssignment(splitGenerator.getNextBatch(maxBatchSize)));
            splits = assignment.removeAll(backend);
        }
        return splits;
    }
}
