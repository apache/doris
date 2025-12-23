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

package org.apache.orc.bench.core.filter;

import com.google.auto.service.AutoService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFilterContext;
import org.apache.orc.Reader;
import org.apache.orc.bench.core.OrcBenchmark;
import org.apache.orc.impl.OrcFilterContextImpl;
import org.apache.orc.impl.filter.FilterFactory;
import org.apache.orc.impl.filter.RowFilterFactory;
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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.orc.bench.core.BenchmarkOptions.FORK;
import static org.apache.orc.bench.core.BenchmarkOptions.GC;
import static org.apache.orc.bench.core.BenchmarkOptions.HELP;
import static org.apache.orc.bench.core.BenchmarkOptions.ITERATIONS;
import static org.apache.orc.bench.core.BenchmarkOptions.MAX_MEMORY;
import static org.apache.orc.bench.core.BenchmarkOptions.MIN_MEMORY;
import static org.apache.orc.bench.core.BenchmarkOptions.TIME;
import static org.apache.orc.bench.core.BenchmarkOptions.WARMUP_ITERATIONS;

@AutoService(OrcBenchmark.class)
public class FilterBench implements OrcBenchmark {
  @Override
  public String getName() {
    return "filter";
  }

  @Override
  public String getDescription() {
    return "Perform filter bench";
  }

  @Override
  public void run(String[] args) throws Exception {
    new Runner(parseOptions(args)).run();
  }

  public static CommandLine parseCommandLine(String[] args, boolean needsArgs) {
    org.apache.commons.cli.Options options = new org.apache.commons.cli.Options()
        .addOption("h", HELP, false, "Provide help")
        .addOption("i", ITERATIONS, true, "Number of iterations")
        .addOption("I", WARMUP_ITERATIONS, true, "Number of warmup iterations")
        .addOption("f", FORK, true, "How many forks to use")
        .addOption("t", TIME, true, "How long each iteration is in seconds")
        .addOption("m", MIN_MEMORY, true, "The minimum size of each JVM")
        .addOption("M", MAX_MEMORY, true, "The maximum size of each JVM")
        .addOption("g", GC, false, "Should GC be profiled");
    CommandLine result;
    try {
      result = new DefaultParser().parse(options, args, true);
    } catch (ParseException pe) {
      System.err.println("Argument exception - " + pe.getMessage());
      result = null;
    }
    if (result == null || result.hasOption(HELP) || (needsArgs && result.getArgs().length == 0)) {
      new HelpFormatter().printHelp("java -jar <jar> <command> <options> <sub_cmd>\n"
                                    + "sub_cmd:\nsimple\ncomplex\n",
                                    options);
      System.err.println();
      System.exit(1);
    }
    return result;
  }

  public static OptionsBuilder optionsBuilder(CommandLine options) {
    OptionsBuilder builder = new OptionsBuilder();
    if (options.hasOption(GC)) {
      builder.addProfiler("hs_gc");
    }
    if (options.hasOption(ITERATIONS)) {
      builder.measurementIterations(Integer.parseInt(options.getOptionValue(ITERATIONS)));
    }
    if (options.hasOption(WARMUP_ITERATIONS)) {
      builder.warmupIterations(Integer.parseInt(options.getOptionValue(
          WARMUP_ITERATIONS)));
    }
    if (options.hasOption(FORK)) {
      builder.forks(Integer.parseInt(options.getOptionValue(
          FORK)));
    }
    if (options.hasOption(TIME)) {
      TimeValue iterationTime = TimeValue.seconds(Long.parseLong(
          options.getOptionValue(TIME)));
      builder.measurementTime(iterationTime);
      builder.warmupTime(iterationTime);
    }

    String minMemory = options.getOptionValue(MIN_MEMORY, "256m");
    String maxMemory = options.getOptionValue(MAX_MEMORY, "2g");
    builder.jvmArgs("-server",
                    "-Xms" + minMemory, "-Xmx" + maxMemory);
    return builder;
  }

  public static Options parseOptions(String[] args) {
    CommandLine options = parseCommandLine(args, true);
    OptionsBuilder builder = optionsBuilder(options);
    String cmd = options.getArgs()[0];
    switch (cmd) {
      case "simple":
        builder.include(SimpleFilter.class.getSimpleName());
        break;
      case "complex":
        builder.include(ComplexFilter.class.getSimpleName());
        break;
      default:
        throw new UnsupportedOperationException(String.format("Command %s is not supported", cmd));
    }
    return builder.build();
  }

  private static Consumer<OrcFilterContext> createFilter(SearchArgument sArg,
                                                         String fType,
                                                         boolean normalize,
                                                         Configuration conf)
    throws FilterFactory.UnSupportedSArgException {
    switch (fType) {
      case "row":
        return RowFilterFactory.create(sArg,
                                       FilterBenchUtil.schema,
                                       OrcFile.Version.CURRENT,
                                       normalize);
      case "vector":
        Reader.Options options = new Reader.Options(conf)
            .searchArgument(sArg, new String[0])
            .allowSARGToFilter(true)
            .useSelected(true);
        return FilterFactory.createBatchFilter(options,
                                               FilterBenchUtil.schema,
                                               false,
                                               OrcFile.Version.CURRENT,
                                               normalize,
                                               null,
                                               null);
      default:
        throw new IllegalArgumentException();
    }
  }

  @OutputTimeUnit(value = TimeUnit.MICROSECONDS)
  @Warmup(iterations = 20, time = 1)
  @BenchmarkMode(value = Mode.AverageTime)
  @Fork(value = 1)
  @State(value = Scope.Benchmark)
  @Measurement(iterations = 20, time = 1)
  public static class SimpleFilter {
    private OrcFilterContext fc;
    private int[] expSel;

    @Param( {"4", "8", "16", "32", "256"})
    private int fInSize;

    @Param( {"row", "vector"})
    private String fType;

    private Consumer<OrcFilterContext> f;

    @Setup
    public void setup() throws FilterFactory.UnSupportedSArgException {
      Random rnd = new Random(1024);
      VectorizedRowBatch b = FilterBenchUtil.createBatch(rnd);
      Configuration conf = new Configuration();
      fc = new OrcFilterContextImpl(FilterBenchUtil.schema, false).setBatch(b);
      Map.Entry<SearchArgument, int[]> r = FilterBenchUtil.createSArg(rnd, b, fInSize);
      SearchArgument sArg = r.getKey();
      expSel = r.getValue();
      f = createFilter(sArg, fType, false, conf);
    }

    @Benchmark
    public OrcFilterContext filter() {
      // Reset the selection
      FilterBenchUtil.unFilterBatch(fc);
      f.accept(fc);
      return fc;
    }

    @TearDown
    public void tearDown() {
      FilterBenchUtil.validate(fc, expSel);
    }
  }

  @OutputTimeUnit(value = TimeUnit.MICROSECONDS)
  @Warmup(iterations = 20, time = 1)
  @BenchmarkMode(value = Mode.AverageTime)
  @Fork(value = 1)
  @State(value = Scope.Benchmark)
  @Measurement(iterations = 20, time = 1)
  public static class ComplexFilter {
    private OrcFilterContext fc;
    private int[] expSel;

    private final int inSize = 32;

    @Param( {"2", "4", "8"})
    private int fSize;

    @Param( {"true", "false"})
    private boolean normalize;

    @Param( {"row", "vector"})
    private String fType;

    private Consumer<OrcFilterContext> f;
    private final Configuration conf = new Configuration();

    @Setup
    public void setup() throws FilterFactory.UnSupportedSArgException {
      VectorizedRowBatch b = FilterBenchUtil.createBatch(new Random(1024));

      fc = new OrcFilterContextImpl(FilterBenchUtil.schema, false).setBatch(b);
      Map.Entry<SearchArgument, int[]> r = FilterBenchUtil.createComplexSArg(new Random(1024),
                                                                             b,
                                                                             inSize,
                                                                             fSize);

      SearchArgument sArg = r.getKey();
      expSel = r.getValue();
      f = createFilter(sArg, fType, normalize, conf);
    }

    @Benchmark
    public OrcFilterContext filter() {
      // Reset the selection
      FilterBenchUtil.unFilterBatch(fc);
      f.accept(fc);
      return fc;
    }

    @TearDown
    public void tearDown() {
      FilterBenchUtil.validate(fc, expSel);
    }
  }
}
