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

package org.apache.doris.cache;

/**
 * Basic statistics for cache efficiency
 */
public class CacheStats {
    /**
     * Accumulated number of cache hits
     */
    private final long numHits;

    /**
     * Accumulated number of cache misses
     */
    private final long numMisses;

    /**
     * Accumulated number of cache size by entries
     */
    private final long size;

    /**
     * Accumulated number of cache size by bytes
     */
    private final long sizeInBytes;

    /**
     * Accumulated number of cache evictions
     */
    private final long numEvictions;

    /**
     * Accumulated number of cache timeouts
     */
    private final long numTimeouts;

    /**
     * Accumulated number of cache errors
     */
    private final long numErrors;

    public CacheStats(
            long numHits,
            long numMisses,
            long size,
            long sizeInBytes,
            long numEvictions,
            long numTimeouts,
            long numErrors
    ) {
        this.numHits = numHits;
        this.numMisses = numMisses;
        this.size = size;
        this.sizeInBytes = sizeInBytes;
        this.numEvictions = numEvictions;
        this.numTimeouts = numTimeouts;
        this.numErrors = numErrors;
    }

    public long getNumHits() {
        return numHits;
    }

    public long getNumMisses() {
        return numMisses;
    }

    public long getNumEntries() {
        return size;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public long getNumEvictions() {
        return numEvictions;
    }

    public long getNumTimeouts() {
        return numTimeouts;
    }

    public long getNumErrors() {
        return numErrors;
    }

    public long numLookups() {
        return numHits + numMisses;
    }

    public double hitRate() {
        long lookups = numLookups();
        return lookups == 0 ? 0 : numHits / (double) lookups;
    }

    public long averageBytes() {
        return size == 0 ? 0 : sizeInBytes / size;
    }

    public CacheStats delta(CacheStats oldStats) {
        if (oldStats == null) {
            return this;
        }
        return new CacheStats(
                numHits - oldStats.numHits,
                numMisses - oldStats.numMisses,
                size - oldStats.size,
                sizeInBytes - oldStats.sizeInBytes,
                numEvictions - oldStats.numEvictions,
                numTimeouts - oldStats.numTimeouts,
                numErrors - oldStats.numErrors
        );
    }
}
