/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.bench.core;

import java.util.Arrays;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;

/**
 * A driver tool to call the various benchmark classes.
 */
public class Driver {
  private static final ServiceLoader<OrcBenchmark> loader =
      ServiceLoader.load(OrcBenchmark.class);

  private static Map<String, OrcBenchmark> getBenchmarks() {
    Map<String, OrcBenchmark> result = new TreeMap<>();
    for(OrcBenchmark bench: loader) {
      result.put(bench.getName(), bench);
    }
    return result;
  }

  private static final String PATTERN = "  %10s - %s";

  private static void printUsageAndExit(Map<String, OrcBenchmark> benchmarks) {
    System.err.println("Commands:");
    for(OrcBenchmark bench: benchmarks.values()) {
      System.err.println(String.format(PATTERN, bench.getName(),
          bench.getDescription()));
    }
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    Map<String, OrcBenchmark> benchmarks = getBenchmarks();
    if (args.length == 0) {
      printUsageAndExit(benchmarks);
    }
    String command = args[0];
    args = Arrays.copyOfRange(args, 1, args.length);
    OrcBenchmark bench = benchmarks.get(command);
    if (bench == null) {
      printUsageAndExit(benchmarks);
      System.exit(1);
    }
    bench.run(args);
  }
}
