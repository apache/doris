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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class BenchmarkOptions {

  public static final String HELP = "help";
  public static final String ITERATIONS = "iterations";
  public static final String WARMUP_ITERATIONS = "warmup-iterations";
  public static final String FORK = "fork";
  public static final String TIME = "time";
  public static final String MIN_MEMORY = "min-memory";
  public static final String MAX_MEMORY = "max-memory";
  public static final String GC = "gc";
  public static final String STACK_PROFILE = "stack";

  public static CommandLine parseCommandLine(String[] args) {
    Options options = new Options()
        .addOption("h", HELP, false, "Provide help")
        .addOption("i", ITERATIONS, true, "Number of iterations")
        .addOption("I", WARMUP_ITERATIONS, true, "Number of warmup iterations")
        .addOption("f", FORK, true, "How many forks to use")
        .addOption("t", TIME, true, "How long each iteration is in seconds")
        .addOption("m", MIN_MEMORY, true, "The minimum size of each JVM")
        .addOption("M", MAX_MEMORY, true, "The maximum size of each JVM")
        .addOption("p", STACK_PROFILE, false, "Should enable stack profiler in JMH")
        .addOption("g", GC, false, "Should GC be profiled");
    CommandLine result;
    try {
      result = new DefaultParser().parse(options, args, true);
    } catch (ParseException pe) {
      System.err.println("Argument exception - " + pe.getMessage());
      result = null;
    }
    if (result == null || result.hasOption(HELP) || result.getArgs().length == 0) {
      new HelpFormatter().printHelp("java -jar <jar> <command> <options> <data>",
          options);
      System.err.println();
      System.exit(1);
    }
    return result;
  }
}
