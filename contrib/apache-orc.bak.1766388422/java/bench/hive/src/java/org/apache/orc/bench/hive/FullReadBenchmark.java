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
import com.google.gson.JsonStreamParser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
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
import org.apache.orc.bench.core.CompressionKind;
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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@AutoService(OrcBenchmark.class)
public class FullReadBenchmark implements OrcBenchmark {

  private static final Path root = Utilities.getBenchmarkRoot();

  @Param({"taxi", "sales", "github"})
  public String dataset;

  @Param({"gz", "snappy", "zstd"})
  public String compression;

  @Override
  public String getName() {
    return "read-all";
  }

  @Override
  public String getDescription() {
    return "read all columns and rows";
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
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = schema.createRowBatch();
    while (rows.nextBatch(batch)) {
      counters.addRecords(batch.size);
    }
    rows.close();
    counters.addBytes(statistics.getReadOps(), statistics.getBytesRead());
    counters.addInvocation();
  }

  @Benchmark
  public void avro(IOCounters counters) throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.track.impl", TrackingLocalFileSystem.class.getName());
    conf.set("fs.defaultFS", "track:///");
    Path path = Utilities.getVariant(root, dataset, "avro", compression);
    FileSystem.Statistics statistics = FileSystem.getStatistics("track:///",
        TrackingLocalFileSystem.class);
    statistics.reset();
    FsInput file = new FsInput(path, conf);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> dataFileReader =
        new DataFileReader<>(file, datumReader);
    GenericRecord record = null;
    while (dataFileReader.hasNext()) {
      record = dataFileReader.next(record);
      counters.addRecords(1);
    }
    counters.addBytes(statistics.getReadOps(), statistics.getBytesRead());
    counters.addInvocation();
  }

  @Benchmark
  public void parquet(IOCounters counters) throws Exception {
    JobConf conf = new JobConf();
    conf.set("fs.track.impl", TrackingLocalFileSystem.class.getName());
    conf.set("fs.defaultFS", "track:///");
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

  @Benchmark
  public void json(IOCounters counters) throws Exception {
    Configuration conf = new Configuration();
    TrackingLocalFileSystem fs = new TrackingLocalFileSystem();
    fs.initialize(new URI("file:///"), conf);
    FileSystem.Statistics statistics = fs.getLocalStatistics();
    statistics.reset();
    Path path = Utilities.getVariant(root, dataset, "json", compression);
    CompressionKind compress = CompressionKind.fromExtension(compression);
    InputStream input = compress.read(fs.open(path));
    JsonStreamParser parser =
        new JsonStreamParser(new InputStreamReader(input,
            StandardCharsets.UTF_8));
    while (parser.hasNext()) {
      parser.next();
      counters.addRecords(1);
    }
    counters.addBytes(statistics.getReadOps(), statistics.getBytesRead());
    counters.addInvocation();
  }
}
