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

package org.apache.orc.bench.spark;

import com.google.auto.service.AutoService;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrackingLocalFileSystem;
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.core.IOCounters;
import org.apache.orc.bench.core.OrcBenchmark;
import org.apache.orc.bench.core.Utilities;
import org.apache.orc.bench.core.convert.GenerateVariants;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.AvroFileFormat;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat;
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.sources.And$;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThanOrEqual$;
import org.apache.spark.sql.sources.LessThan$;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@AutoService(OrcBenchmark.class)
public class SparkBenchmark implements OrcBenchmark {

  private static final Path root = Utilities.getBenchmarkRoot();

  @Override
  public String getName() {
    return "spark";
  }

  @Override
  public String getDescription() {
    return "Run Spark benchmarks";
  }

  @Override
  public void run(String[] args) throws Exception {
    CommandLine cmds = GenerateVariants.parseCommandLine(args);
    new Runner(new OptionsBuilder()
        .parent(Utilities.parseOptions(args, this.getClass()))
        .param("compression", cmds.getOptionValue("compress", "gz,snappy,zstd").split(","))
        .param("dataset", cmds.getOptionValue("data", "taxi,sales,github").split(","))
        .param("format", cmds.getOptionValue("format", "orc,parquet,json").split(","))
        .build()
    ).run();
  }

  @State(Scope.Thread)
  public static class InputSource {
    SparkSession session;
    TrackingLocalFileSystem fs;
    Configuration conf;
    Path path;
    StructType schema;
    StructType empty = new StructType();
    FileFormat formatObject;

    @Param({"taxi", "sales", "github"})
    String dataset;

    @Param({"gz", "snappy", "zstd"})
    String compression;

    @Param({"orc", "parquet", "json"})
    String format;

    @Setup(Level.Trial)
    public void setup() {
      session = SparkSession.builder().appName("benchmark")
          .config("spark.master", "local[4]")
          .config("spark.sql.orc.filterPushdown", true)
          .config("spark.sql.orc.impl", "native")
          .getOrCreate();
      conf = session.sparkContext().hadoopConfiguration();
      conf.set("avro.mapred.ignore.inputs.without.extension","false");
      conf.set("fs.track.impl", TrackingLocalFileSystem.class.getName());
      path = new Path("track://",
          Utilities.getVariant(root, dataset, format, compression));
      try {
        fs = (TrackingLocalFileSystem) path.getFileSystem(conf);
      } catch (IOException e) {
        throw new IllegalArgumentException("Can't get filesystem", e);
      }
      try {
        TypeDescription orcSchema = Utilities.loadSchema(dataset + ".schema");
        schema = (StructType) SparkSchema.convertToSparkType(orcSchema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Can't read schema " + dataset, e);
      }
      switch (format) {
        case "avro":
          formatObject = new AvroFileFormat();
          break;
        case "orc":
          formatObject = new OrcFileFormat();
          break;
        case "parquet":
          formatObject = new ParquetFileFormat();
          break;
        case "json":
          formatObject = new JsonFileFormat();
          break;
        default:
          throw new IllegalArgumentException("Unknown format " + format);
      }
    }
  }

  static void processReader(Iterator<InternalRow> reader,
                            FileSystem.Statistics statistics,
                            IOCounters counters,
                            Blackhole blackhole) {
    while (reader.hasNext()) {
      Object row = reader.next();
      if (row instanceof ColumnarBatch) {
        counters.addRecords(((ColumnarBatch) row).numRows());
      } else {
        counters.addRecords(1);
      }
      blackhole.consume(row);
    }
    counters.addInvocation();
    counters.addBytes(statistics.getReadOps(), statistics.getBytesRead());
  }

  @Benchmark
  public void fullRead(InputSource source,
                       IOCounters counters,
                       Blackhole blackhole) {
    FileSystem.Statistics statistics = source.fs.getLocalStatistics();
    statistics.reset();
    List<Filter> filters = new ArrayList<>();
    List<Tuple2<String,String>> options = new ArrayList<>();
    switch (source.format) {
      case "json":
        options.add(new Tuple2<>("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS"));
        break;
      default:
        break;
    }
    Seq<Tuple2<String,String>> optionsScala = JavaConverters
        .asScalaBufferConverter(options).asScala().toSeq();
    @SuppressWarnings("unchecked")
    Map<String,String> scalaMap = (Map<String, String>)Map$.MODULE$.apply(optionsScala);
    Function1<PartitionedFile,Iterator<InternalRow>> factory =
        source.formatObject.buildReaderWithPartitionValues(source.session,
            source.schema, source.empty, source.schema,
            JavaConverters.collectionAsScalaIterableConverter(filters).asScala().toSeq(),
            scalaMap, source.conf);
    PartitionedFile file = new PartitionedFile(InternalRow.empty(),
        source.path.toString(), 0, Long.MAX_VALUE, new String[0], 0L, 0L);
    processReader(factory.apply(file), statistics, counters, blackhole);
  }

  @Benchmark
  public void partialRead(InputSource source,
                          IOCounters counters,
                          Blackhole blackhole) {
    FileSystem.Statistics statistics = source.fs.getLocalStatistics();
    statistics.reset();
    List<Filter> filters = new ArrayList<>();
    List<Tuple2<String,String>> options = new ArrayList<>();
    switch (source.format) {
      case "json":
      case "avro":
        throw new IllegalArgumentException(source.format + " can't handle projection");
      default:
        break;
    }
    TypeDescription readSchema = null;
    switch (source.dataset) {
      case "taxi":
        readSchema = TypeDescription.fromString("struct<vendor_id:int," +
            "pickup_time:timestamp>");
        break;
      case "sales":
        readSchema = TypeDescription.fromString("struct<sales_id:bigint," +
            "customer_id:bigint>");
        break;
      case "github":
        readSchema = TypeDescription.fromString("struct<actor:struct<" +
            "avatar_url:string,gravatar_id:string,id:int,login:string,url:string>," +
            "created_at:timestamp>");
        break;
      default:
        throw new IllegalArgumentException("Unknown data set " + source.dataset);
    }
    Seq<Tuple2<String,String>> optionsScala =
        JavaConverters.asScalaBufferConverter(options).asScala().toSeq();
    @SuppressWarnings("unchecked")
    Map<String,String> scalaMap = (Map<String, String>)Map$.MODULE$.apply(optionsScala);
    Function1<PartitionedFile,Iterator<InternalRow>> factory =
        source.formatObject.buildReaderWithPartitionValues(source.session,
            source.schema, source.empty,
            (StructType) SparkSchema.convertToSparkType(readSchema),
            JavaConverters.collectionAsScalaIterableConverter(filters).asScala().toSeq(),
            scalaMap, source.conf);
    PartitionedFile file = new PartitionedFile(InternalRow.empty(),
        source.path.toString(), 0, Long.MAX_VALUE, new String[0], 0L, 0L);
    processReader(factory.apply(file), statistics, counters, blackhole);
  }

  @Benchmark
  public void pushDown(InputSource source,
                       IOCounters counters,
                       Blackhole blackhole) {
    FileSystem.Statistics statistics = source.fs.getLocalStatistics();
    statistics.reset();
    List<Filter> filters = new ArrayList<>();
    switch (source.dataset) {
      case "taxi":
        filters.add(And$.MODULE$.apply(
            GreaterThanOrEqual$.MODULE$.apply("pickup_time",
                Timestamp.valueOf("2015-11-01 00:00:00.0")),
            LessThan$.MODULE$.apply("pickup_time",
                Timestamp.valueOf("2015-11-01 00:01:00.0"))));
        break;
      case "sales":
        filters.add(And$.MODULE$.apply(
            GreaterThanOrEqual$.MODULE$.apply("sales_id", 1000000000L),
            LessThan$.MODULE$.apply("sales_id", 1000001000L)));
        break;
      case "github":
        filters.add(And$.MODULE$.apply(
            GreaterThanOrEqual$.MODULE$.apply("created_at",
                Timestamp.valueOf("2015-11-01 00:00:00.0")),
            LessThan$.MODULE$.apply("created_at",
                Timestamp.valueOf("2015-11-01 00:01:00.0"))));
        break;
      default:
        throw new IllegalArgumentException("Unknown data set " + source.dataset);
    }
    List<Tuple2<String,String>> options = new ArrayList<>();
    switch (source.format) {
      case "json":
      case "avro":
        throw new IllegalArgumentException(source.format + " can't handle pushdown");
      default:
        break;
    }
    Seq<Tuple2<String,String>> optionsScala =
        JavaConverters.asScalaBufferConverter(options).asScala().toSeq();
    @SuppressWarnings("unchecked")
    Map<String,String> scalaMap = (Map<String, String>)Map$.MODULE$.apply(optionsScala);
    Function1<PartitionedFile,Iterator<InternalRow>> factory =
        source.formatObject.buildReaderWithPartitionValues(source.session,
            source.schema, source.empty, source.schema,
            JavaConverters.collectionAsScalaIterableConverter(filters).asScala().toSeq(),
            scalaMap, source.conf);
    PartitionedFile file = new PartitionedFile(InternalRow.empty(),
        source.path.toString(), 0, Long.MAX_VALUE, new String[0], 0L, 0L);
    processReader(factory.apply(file), statistics, counters, blackhole);
  }
}
