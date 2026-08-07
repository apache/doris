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

import org.apache.doris.connector.iceberg.IcebergPartitionCache.CachedPartitions;
import org.apache.doris.connector.iceberg.IcebergPartitionCache.Key;
import org.apache.doris.connector.iceberg.IcebergPartitionUtils.IcebergRawPartition;

import org.apache.iceberg.catalog.TableIdentifier;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jol.info.GraphLayout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Compares the O(1) production partition weight with construction-time accounting and JOL traversal. */
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 1, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Xms1g", "-Xmx4g", "-Djol.magicFieldOffset=true"})
public class IcebergPartitionSizeBenchmark {

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public long cachedWeight(BenchmarkState state) {
        return IcebergCacheSizeEstimator.estimatePartitionEntry(state.key, state.value);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long constructAndEstimate(BenchmarkState state) {
        return new CachedPartitions(state.partitions).estimatedBytes;
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long jolRetainedGraph(BenchmarkState state) {
        return GraphLayout.parseInstance(state.key, state.value).totalSize();
    }

    @State(Scope.Thread)
    public static class BenchmarkState {
        @Param({"10000", "100000"})
        public int size;

        private Key key;
        private CachedPartitions value;
        private List<IcebergRawPartition> partitions;

        @Setup(Level.Trial)
        public void setup() {
            partitions = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                List<String> columns = new ArrayList<>(1);
                columns.add("dt");
                List<String> values = new ArrayList<>(1);
                values.add("2026-08-" + (i % 28 + 1));
                List<String> transforms = new ArrayList<>(1);
                transforms.add("identity");
                partitions.add(new IcebergRawPartition(
                        "dt=" + values.get(0) + "/bucket=" + i,
                        columns,
                        values,
                        transforms,
                        1_786_048_000_000L + i,
                        10_000L + i));
            }
            key = new Key(TableIdentifier.of("db", "table"), 10_000L);
            value = new CachedPartitions(partitions);
        }
    }
}
