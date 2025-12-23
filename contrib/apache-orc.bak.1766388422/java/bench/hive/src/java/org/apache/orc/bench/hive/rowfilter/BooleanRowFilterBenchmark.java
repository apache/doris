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
package org.apache.orc.bench.hive.rowfilter;

import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BooleanRowFilterBenchmark extends org.openjdk.jmh.Main {

  @State(Scope.Thread)
  public static class InputState extends RowFilterInputState {

    @Param({"ORIGINAL"})
    public TypeDescription.RowBatchVersion version;

    @Param({"BOOLEAN"})
    public TypeDescription.Category benchType;

    @Param({"0.01", "0.1", "0.2", "0.4", "0.6", "0.8", "1."})
    public String filterPerc;

    @Param({"2"})
    public int filterColsNum;

    String dataRelativePath = "data/generated/sales/orc.zstd";

    String schemaName = "sales.schema";

    String filterColumn = "sales_id";

  }

  @Benchmark
  public void readOrcRowFilter(Blackhole blackhole, InputState state) throws Exception {
    RecordReader rows =
        state.reader.rows(state.readerOptions
            .setRowFilter(new String[]{state.filterColumn}, state::customIntRowFilter));
    while (rows.nextBatch(state.batch)) {
      blackhole.consume(state.batch);
    }
    rows.close();
  }

  @Benchmark
  public void readOrcNoFilter(Blackhole blackhole, InputState state) throws Exception {
    RecordReader rows = state.reader.rows(state.readerOptions);
    while (rows.nextBatch(state.batch)) {
      blackhole.consume(state.batch);
    }
    rows.close();
  }

  /*
   * Run this test:
   *  java -cp hive/target/orc-benchmarks-hive-*-uber.jar org.apache.orc.bench.hive.rowfilter.BooleanRowFilterBenchmark
   */
  public static void main(String[] args) throws RunnerException {
    new Runner(new OptionsBuilder()
        .include(BooleanRowFilterBenchmark.class.getSimpleName())
        .forks(1)
        .build()).run();
  }
}
