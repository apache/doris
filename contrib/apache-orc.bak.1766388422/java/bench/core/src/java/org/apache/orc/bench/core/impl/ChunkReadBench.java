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

package org.apache.orc.bench.core.impl;

import com.google.auto.service.AutoService;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.bench.core.OrcBenchmark;
import org.apache.orc.bench.core.filter.FilterBench;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@OutputTimeUnit(value = TimeUnit.SECONDS)
@Warmup(iterations = 20, time = 1)
@BenchmarkMode(value = Mode.AverageTime)
@Fork(value = 1)
@State(value = Scope.Benchmark)
@Measurement(iterations = 20, time = 1)
@AutoService(OrcBenchmark.class)
public class ChunkReadBench implements OrcBenchmark {
  @Override
  public String getName() {
    return "chunk_read";
  }

  @Override
  public String getDescription() {
    return "Perform chunk read bench";
  }

  @Override
  public void run(String[] args) throws Exception {
    CommandLine options = FilterBench.parseCommandLine(args, false);
    OptionsBuilder builder = FilterBench.optionsBuilder(options);
    builder.include(getClass().getSimpleName());
    new Runner(builder.build()).run();
  }

  private final Path workDir = new Path(System.getProperty("test.tmp.dir",
                                                           "target" + File.separator + "test"
                                                           + File.separator + "tmp"));
  private final Path filePath = new Path(workDir, "perf_chunk_read_file.orc");
  private final Configuration conf = new Configuration();
  @Param( {"128"})
  private int colCount;

  @Param( {"65536"})
  private int rowCount;

  @Param( {"true", "false"})
  private boolean alternate;

  @Param( {"0", "4194304"})
  private int minSeekSize;

  @Param( {"0.0", "10.0"})
  private double extraByteTolerance;

  private long readRows = 0;

  @Setup
  public void setup() throws IOException {
    if (minSeekSize == 0 && extraByteTolerance > 0) {
      throw new IllegalArgumentException("Ignore extraByteTolerance variations with seekSize is"
                                         + " 0");
    }
    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(filePath)) {
      ChunkReadUtil.createORCFile(colCount, rowCount, filePath);
    }
    ChunkReadUtil.setConf(conf, minSeekSize, extraByteTolerance);
  }

  @Benchmark
  public long read() throws IOException {
    readRows = ChunkReadUtil.readORCFile(filePath, conf, alternate);
    return readRows;
  }

  @TearDown
  public void tearDown() {
    if (readRows != rowCount) {
      throw new IllegalArgumentException(String.format(
        "readRows %d is not equal to expected rows %d", readRows, rowCount));
    }
  }
}
