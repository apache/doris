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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.PartitionSpec;
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

/** Compares the O(1) production manifest weight with construction-time accounting and JOL traversal. */
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 1, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Xms1g", "-Xmx4g", "-Djol.magicFieldOffset=true"})
public class IcebergManifestSizeBenchmark {

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public long cachedWeight(BenchmarkState state) {
        return IcebergCacheSizeEstimator.estimateManifestEntry(state.key, state.value);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long constructAndEstimate(BenchmarkState state) {
        return ManifestCacheValue.forDataFiles(state.files).getEstimatedBytes();
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

        private IcebergManifestEntryKey key;
        private ManifestCacheValue value;
        private List<DataFile> files;

        @Setup(Level.Trial)
        public void setup() {
            PartitionSpec spec = PartitionSpec.unpartitioned();
            files = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                files.add(DataFiles.builder(spec)
                        .withPath("s3://warehouse/db/table/data/part-" + i + ".parquet")
                        .withFileSizeInBytes(128L * 1024L * 1024L + i)
                        .withRecordCount(1_000_000L + i)
                        .build());
            }
            key = new IcebergManifestEntryKey(
                    "s3://warehouse/db/table/metadata/manifest.avro", ManifestContent.DATA);
            value = ManifestCacheValue.forDataFiles(files);
        }
    }
}
