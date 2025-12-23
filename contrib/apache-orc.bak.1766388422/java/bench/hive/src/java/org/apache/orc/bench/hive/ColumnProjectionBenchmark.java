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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.core.IOCounters;
import org.apache.orc.bench.core.OrcBenchmark;
import org.apache.orc.bench.core.Utilities;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;

import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@AutoService(OrcBenchmark.class)
public class ColumnProjectionBenchmark implements OrcBenchmark {

  private static final Path root = Utilities.getBenchmarkRoot();

  @Param({ "github", "sales", "taxi"})
  public String dataset;

  @Param({"snappy", "gz", "zstd"})
  public String compression;

  @Override
  public String getName() {
    return "read-some";
  }

  @Override
  public String getDescription() {
    return "Benchmark column projection";
  }

  @Override
  public void run(String[] args) throws Exception {
    new Runner(Utilities.parseOptions(args, getClass())).run();
  }

  @Benchmark
  public void orc(IOCounters counters) throws Exception{
    Configuration conf = new Configuration();
    TrackingLocalFileSystem fs = new TrackingLocalFileSystem();
    fs.initialize(new URI("file:///"), conf);
    FileSystem.Statistics statistics = fs.getLocalStatistics();
    statistics.reset();
    OrcFile.ReaderOptions options = OrcFile.readerOptions(conf).filesystem(fs);
    Path path = Utilities.getVariant(root, dataset, "orc", compression);
    Reader reader = OrcFile.createReader(path, options);
    TypeDescription schema = reader.getSchema();
    boolean[] include = new boolean[schema.getMaximumId() + 1];
    // select first two columns
    List<TypeDescription> children = schema.getChildren();
    for(int c= children.get(0).getId(); c <= children.get(1).getMaximumId(); ++c) {
      include[c] = true;
    }
    RecordReader rows = reader.rows(new Reader.Options()
        .include(include));
    VectorizedRowBatch batch = schema.createRowBatch();
    while (rows.nextBatch(batch)) {
      counters.addRecords(batch.size);
    }
    rows.close();
    counters.addBytes(statistics.getReadOps(), statistics.getBytesRead());
    counters.addInvocation();
  }

  @Benchmark
  public void parquet(IOCounters counters) throws Exception {
    JobConf conf = new JobConf();
    conf.set("fs.track.impl", TrackingLocalFileSystem.class.getName());
    conf.set("fs.defaultFS", "track:///");
    if ("taxi".equals(dataset)) {
      conf.set("columns", "vendor_id,pickup_time");
      conf.set("columns.types", "int,timestamp");
    } else if ("sales".equals(dataset)) {
      conf.set("columns", "sales_id,customer_id");
      conf.set("columns.types", "bigint,bigint");
    } else if ("github".equals(dataset)) {
      conf.set("columns", "actor,created_at");
      conf.set("columns.types", "struct<avatar_url:string,gravatar_id:string," +
          "id:int,login:string,url:string>,timestamp");
    } else {
      throw new IllegalArgumentException("Unknown data set " + dataset);
    }
    Path path = Utilities.getVariant(root, dataset, "parquet", compression);
    FileSystem.Statistics statistics = FileSystem.getStatistics("track:///",
        TrackingLocalFileSystem.class);
    statistics.reset();
    ParquetInputFormat<ArrayWritable> inputFormat =
        new ParquetInputFormat<>(DataWritableReadSupport.class);

    NullWritable nada = NullWritable.get();
    FileSplit split = new FileSplit(path, 0, Long.MAX_VALUE, new String[]{});
    org.apache.hadoop.mapred.RecordReader<NullWritable,ArrayWritable> recordReader =
        new ParquetRecordReaderWrapper(inputFormat, split, conf, Reporter.NULL);
    ArrayWritable value = recordReader.createValue();
    while (recordReader.next(nada, value)) {
      counters.addRecords(1);
    }
    recordReader.close();
    counters.addBytes(statistics.getReadOps(), statistics.getBytesRead());
    counters.addInvocation();
  }
}
