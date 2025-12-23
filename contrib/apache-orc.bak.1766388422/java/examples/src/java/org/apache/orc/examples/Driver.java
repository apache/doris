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

package org.apache.orc.examples;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

/**
 * Driver program for the java ORC examples.
 */
public class Driver {

  @SuppressWarnings("static-access")
  static Options createOptions() {
    Options result = new Options();

    result.addOption("h", "help", false, "Print help message");
    result.addOption("D", "define", true, "Set a configuration property");
    return result;
  }

  static class DriverOptions {
    final CommandLine genericOptions;
    final String command;
    final String[] commandArgs;

    DriverOptions(String[] args) throws ParseException {
      genericOptions = new DefaultParser().parse(createOptions(), args, true);
      String[] unprocessed = genericOptions.getArgs();
      if (unprocessed.length == 0) {
        command = null;
        commandArgs = new String[0];
      } else {
        command = unprocessed[0];
        if (genericOptions.hasOption('h')) {
          commandArgs = new String[]{"-h"};
        } else {
          commandArgs = new String[unprocessed.length - 1];
          System.arraycopy(unprocessed, 1, commandArgs, 0, commandArgs.length);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    DriverOptions options = new DriverOptions(args);

    if (options.command == null) {
      System.err.println("ORC Java Examples");
      System.err.println();
      System.err.println("usage: java -jar orc-examples-*.jar [--help]" +
          " [--define X=Y] <command> <args>");
      System.err.println();
      System.err.println("Commands:");
      System.err.println("   write - write a sample ORC file");
      System.err.println("   read - read a sample ORC file");
      System.err.println("   write2 - write a sample ORC file with a map");
      System.err.println("   read2 - read a sample ORC file with a map");
      System.err.println("   compressWriter - write a ORC file with snappy compression");
      System.err.println("   inMemoryEncryptionWriter - write a ORC file with encryption");
      System.err.println("   inMemoryEncryptionReader - read a ORC file with encryption");
      System.err.println();
      System.err.println("To get more help, provide -h to the command");
      System.exit(1);
    }
    Configuration conf = new Configuration();
    String[] confSettings = options.genericOptions.getOptionValues("D");
    if (confSettings != null) {
      for (String param : confSettings) {
        String[] parts = param.split("=", 2);
        conf.set(parts[0], parts[1]);
      }
    }
    if ("read".equals(options.command)) {
      CoreReader.main(conf, options.commandArgs);
    } else if ("write".equals(options.command)) {
      CoreWriter.main(conf, options.commandArgs);
    } else if ("write2".equals(options.command)) {
      AdvancedWriter.main(conf, options.commandArgs);
    } else if ("read2".equals(options.command)) {
      AdvancedReader.main(conf, options.commandArgs);
    } else if ("compressWriter".equals(options.command)) {
      CompressionWriter.main(conf, options.commandArgs);
    } else if ("inMemoryEncryptionWriter".equals(options.command)) {
      InMemoryEncryptionWriter.main(conf, options.commandArgs);
    } else if ("inMemoryEncryptionReader".equals(options.command)) {
      InMemoryEncryptionReader.main(conf, options.commandArgs);
    } else {
      System.err.println("Unknown subcommand: " + options.command);
      System.exit(1);
    }
  }
}
