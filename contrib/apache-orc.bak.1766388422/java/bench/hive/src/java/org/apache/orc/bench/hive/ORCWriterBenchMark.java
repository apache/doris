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

package org.apache.orc.bench.hive;

import com.google.auto.service.AutoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrackingLocalFileSystem;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.bench.core.IOCounters;
import org.apache.orc.bench.core.OrcBenchmark;
import org.apache.orc.bench.core.Utilities;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@AutoService(OrcBenchmark.class)
public class ORCWriterBenchMark implements OrcBenchmark {
  private static final Path root = Utilities.getBenchmarkRoot();
  private List<VectorizedRowBatch> batches = new ArrayList<>();

  private Path dumpDir() {
    return new Path(root, "dumpDir");
  }

  @Override
  public String getName() {
    return "write";
  }

  @Override
  public String getDescription() {
    return "Benchmark ORC Writer with different DICT options";
  }

  @Setup(Level.Trial)
  public void prepareData() {
    TypeDescription schema = TypeDescription.fromString("struct<str:string>");
    VectorizedRowBatch batch = schema.createRowBatch();
    BytesColumnVector strVector = (BytesColumnVector) batch.cols[0];

    Random rand = new Random();
    for (int i = 0; i < 32 * 1024; i++) {
      if (batch.size == batch.getMaxSize()) {
        batches.add(batch);
        batch = schema.createRowBatch();
        strVector = (BytesColumnVector) batch.cols[0];
      }
      int randomNum = rand.nextInt(distinctCount - 10 + 1) + 10;
      byte[] value = String.format("row %06d", randomNum).getBytes(StandardCharsets.UTF_8);
      strVector.setRef(batch.size, value, 0, value.length);
      ++batch.size;
    }

    batches.add(batch);
  }

  @Param({"RBTREE", "HASH", "NONE"})
  public String dictImpl;

  /**
   * Controlling the bound of randomly generated data which is used to control the number of distinct keys
   * appeared in the dictionary.
   */
  @Param({"10000", "2500", "500"})
  public int distinctCount;


  @TearDown(Level.Invocation)
  public void tearDownBenchmark()
      throws Exception {
    // Cleaning up the dump files.
    Configuration conf = new Configuration();
    TrackingLocalFileSystem fs = new TrackingLocalFileSystem();
    fs.initialize(new URI("file:///"), conf);
    fs.delete(dumpDir(), true);
  }

  @Override
  public void run(String[] args)
      throws Exception {
    new Runner(Utilities.parseOptions(args, getClass())).run();
  }

  @Benchmark
  public void dictBench(IOCounters counters)
      throws Exception {
    Configuration conf = new Configuration();
    if (dictImpl.equalsIgnoreCase("NONE")) {
      // turn off the dictionaries
      OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.setDouble(conf, 0);
    } else {
      OrcConf.DICTIONARY_IMPL.setString(conf, dictImpl);
    }

    TrackingLocalFileSystem fs = new TrackingLocalFileSystem();
    fs.initialize(new URI("file:///"), conf);
    FileSystem.Statistics statistics = fs.getLocalStatistics();
    statistics.reset();
    Path testFilePath = new Path(dumpDir(), "dictBench");

    TypeDescription schema = TypeDescription.fromString("struct<str:string>");
    // Note that the total data volume will be around 100 * 1024 * 1024
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).fileSystem(fs).setSchema(schema).compress(CompressionKind.NONE)
            .stripeSize(128 * 1024));

    for (VectorizedRowBatch batch : batches) {
      writer.addRowBatch(batch);
      counters.addRecords(batch.size);
    }

    writer.close();
    counters.addBytes(statistics.getWriteOps(), statistics.getBytesWritten());
    counters.addInvocation();
  }
}
