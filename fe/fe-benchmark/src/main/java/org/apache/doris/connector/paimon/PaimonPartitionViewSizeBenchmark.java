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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.cache.ConnectorTableKey;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Compares Paimon's O(1) cached partition-view weight with construction-time accounting and JOL traversal. */
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 1, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Xms1g", "-Xmx4g", "-Djol.magicFieldOffset=true"})
public class PaimonPartitionViewSizeBenchmark {

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public long cachedWeight(BenchmarkState state) {
        return PaimonPartitionViewSizeEstimator.estimateEntry(state.key, state.value);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long constructAndEstimate(BenchmarkState state) {
        return new PaimonPartitionView(state.key, state.partitions).getEstimatedBytes();
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

        private ConnectorTableKey key;
        private PaimonPartitionView value;
        private List<ConnectorPartitionInfo> partitions;

        @Setup(Level.Trial)
        public void setup() {
            partitions = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                Map<String, String> values = new LinkedHashMap<>();
                values.put("dt", "2026-08-" + (i % 28 + 1));
                values.put("bucket", Integer.toString(i));
                partitions.add(new ConnectorPartitionInfo(
                        "dt=" + values.get("dt") + "/bucket=" + i,
                        values,
                        Collections.emptyMap(),
                        10_000L + i,
                        128L * 1024L * 1024L + i,
                        1_786_048_000_000L + i,
                        4L,
                        new ArrayList<>(values.values()),
                        Arrays.asList(false, false)));
            }
            key = new ConnectorTableKey("db", "table", 10_000L, -1L);
            value = new PaimonPartitionView(key, partitions);
        }
    }
}
