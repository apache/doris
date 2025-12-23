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

package org.apache.orc.bench.core;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.Path;
import org.apache.orc.TypeDescription;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class Utilities {

  public static TypeDescription loadSchema(String name) throws IOException {
    InputStream in = Utilities.class.getClassLoader().getResourceAsStream(name);
    Preconditions.checkArgument(in != null, "Schema not found: " + name);
    byte[] buffer= new byte[1 * 1024];
    int len = in.read(buffer);
    StringBuilder string = new StringBuilder();
    while (len > 0) {
      for(int i=0; i < len; ++i) {
        // strip out
        if (buffer[i] != '\n' && buffer[i] != ' ') {
          string.append((char) buffer[i]);
        }
      }
      len = in.read(buffer);
    }
    return TypeDescription.fromString(string.toString());
  }

  public static org.apache.orc.CompressionKind getCodec(CompressionKind compression) {
    switch (compression) {
      case NONE:
        return org.apache.orc.CompressionKind.NONE;
      case ZLIB:
        return org.apache.orc.CompressionKind.ZLIB;
      case SNAPPY:
        return org.apache.orc.CompressionKind.SNAPPY;
      case ZSTD:
        return org.apache.orc.CompressionKind.ZSTD;
      default:
        throw new IllegalArgumentException("Unknown compression " + compression);
    }
  }

  public static Path getVariant(Path root,
                                String data,
                                String format,
                                String compress) {
    return new Path(root, "generated/" + data + "/" + format + "." + compress);
  }

  private static final String ROOT_PROPERTY_NAME = "bench.root.dir";

  /**
   * Get the benchmark data root in the child jvm.
   * @return the path to the benchmark data or null if it wasn't found
   */
  public static Path getBenchmarkRoot() {
    String value = System.getProperty(ROOT_PROPERTY_NAME);
    return value == null ? null : new Path(value);
  }

  public static Options parseOptions(String[] args,
                                     Class cls) throws IOException {
    CommandLine options = BenchmarkOptions.parseCommandLine(args);
    String dataPath = new File(options.getArgs()[0]).getCanonicalPath();
    OptionsBuilder builder = new OptionsBuilder();
    builder.include(cls.getSimpleName());
    if (options.hasOption(BenchmarkOptions.GC)) {
      builder.addProfiler("hs_gc");
    }
    if (options.hasOption(BenchmarkOptions.STACK_PROFILE)) {
      builder.addProfiler("stack");
    }
    builder.measurementIterations(Integer.parseInt(options.getOptionValue(
        BenchmarkOptions.ITERATIONS, "5")));
    builder.warmupIterations(Integer.parseInt(options.getOptionValue(
        BenchmarkOptions.WARMUP_ITERATIONS, "2")));
    builder.forks(Integer.parseInt(options.getOptionValue(
        BenchmarkOptions.FORK, "1")));
    TimeValue iterationTime = TimeValue.seconds(Long.parseLong(
        options.getOptionValue(BenchmarkOptions.TIME, "10")));
    builder.measurementTime(iterationTime);
    builder.warmupTime(iterationTime);
    String minMemory = options.getOptionValue(BenchmarkOptions.MIN_MEMORY, "256m");
    String maxMemory = options.getOptionValue(BenchmarkOptions.MAX_MEMORY, "2g");
    builder.jvmArgs("-server",
        "-Xms"+ minMemory, "-Xmx" + maxMemory,
        "-D" + ROOT_PROPERTY_NAME + "=" + dataPath);
    return builder.build();
  }
}
