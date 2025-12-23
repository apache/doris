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

package org.apache.orc.tools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.tools.convert.ConvertTool;
import org.apache.orc.tools.json.JsonSchemaFinder;

import java.util.Map;
import java.util.Properties;

/**
 * Driver program for the java ORC utilities.
 */
public class Driver {

  @SuppressWarnings("static-access")
  static Options createOptions() {
    Options result = new Options();

    result.addOption(Option.builder("h")
        .longOpt("help")
        .desc("Print help message")
        .build());

    result.addOption(Option.builder("D")
        .longOpt("define")
        .desc("Set a configuration property")
        .numberOfArgs(2)
        .valueSeparator()
        .build());
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
      System.err.println("ORC Java Tools");
      System.err.println();
      System.err.println("usage: java -jar orc-tools-*.jar [--help]" +
          " [--define X=Y] <command> <args>");
      System.err.println();
      System.err.println("Commands:");
      System.err.println("   convert - convert CSV and JSON files to ORC");
      System.err.println("   count - recursively find *.orc and print the number of rows");
      System.err.println("   data - print the data from the ORC file");
      System.err.println("   json-schema - scan JSON files to determine their schema");
      System.err.println("   key - print information about the keys");
      System.err.println("   meta - print the metadata about the ORC file");
      System.err.println("   scan - scan the ORC file");
      System.err.println("   sizes - list size on disk of each column");
      System.err.println("   version - print the version of this ORC tool");
      System.err.println();
      System.err.println("To get more help, provide -h to the command");
      System.exit(1);
    }
    Configuration conf = new Configuration();
    Properties confSettings = options.genericOptions.getOptionProperties("D");
    for(Map.Entry pair: confSettings.entrySet()) {
      conf.set(pair.getKey().toString(), pair.getValue().toString());
    }
    switch (options.command) {
      case "convert":
        ConvertTool.main(conf, options.commandArgs);
        break;
      case "count":
        RowCount.main(conf, options.commandArgs);
        break;
      case "data":
        PrintData.main(conf, options.commandArgs);
        break;
      case "json-schema":
        JsonSchemaFinder.main(conf, options.commandArgs);
        break;
      case "key":
        KeyTool.main(conf, options.commandArgs);
        break;
      case "meta":
        FileDump.main(conf, options.commandArgs);
        break;
      case "scan":
        ScanData.main(conf, options.commandArgs);
        break;
      case "sizes":
        ColumnSizes.main(conf, options.commandArgs);
        break;
      case "version":
        PrintVersion.main(conf, options.commandArgs);
        break;
      default:
        System.err.println("Unknown subcommand: " + options.command);
        System.exit(1);
    }
  }
}
